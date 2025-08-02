import threading
import heapq
import functools
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Tuple, List


class TaskExecutor:
    def __init__(self, capacity):
        self.capacity = capacity
        self.tasks = []
        self.lock = threading.Lock()
        self.cond = threading.Condition()
        self.terminated = False
        self._processing_loop()

    def _call_and_release(self, task_func):
        task_func()
        with self.lock:
            self.capacity += 1

        with self.cond:
            self.cond.notify_all()

    def _processing_loop(self):
        def l():
            while True:
                with self.cond:
                    self.cond.wait_for(lambda: self.terminated or (self.capacity > 0 and self.tasks))
                    if self.terminated:
                        print("terminated, breaking out of the loop")
                        break

                    with self.lock:
                        self.capacity -= 1
                        p, task_func = heapq.heappop(self.tasks)

                    print(f"executing a {p=} task")
                    threading.Thread(target=self._call_and_release, args=[task_func]).start()

                '''
                if self.terminated:
                    print("terminated, breaking out of the loop")
                    break

                with self.lock:
                    #print(f"obtained the lock {self.capacity=} {len(self.tasks)=} to execute a task")
                    if self.capacity > 0 and self.tasks:
                        self.capacity -= 1
                        p, task_func = heapq.heappop(self.tasks)
                        print(f"executing a top {p=} task")
                        threading.Thread(target=call_and_release, args=[task_func]).start()
                time.sleep(0.1)
                '''

        self.loop_thread = threading.Thread(target=l)
        self.loop_thread.start()

    def submit_tasks(self, tasks):
        tasks = sorted(tasks)
        with self.lock:
            #for task in tasks:
            #    heapq.heappush(self.tasks, task)
            self.tasks = list(heapq.merge(self.tasks, tasks))

        with self.cond:            
            self.cond.notify_all()

    def terminate(self):
        print("terminate initiated")
        self.terminated = True

        with self.cond:            
            self.cond.notify_all()

class TaskExecutorB:
    """Producer-consumer pattern with PriorityQueue and worker threads"""
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.task_queue = queue.PriorityQueue()
        self.terminated = False
        self.workers = []
        
        # Start worker threads
        for i in range(capacity):
            worker = threading.Thread(target=self._worker_loop, args=[i])
            worker.start()
            self.workers.append(worker)

    def _worker_loop(self, worker_id: int):
        """Worker thread loop - processes tasks from the queue"""
        while True:
            # Get task with timeout to check termination
            priority, task_func = self.task_queue.get()
            
            if task_func is None:  # Poison pill for shutdown
                self.task_queue.task_done()
                break
                
            print(f"TaskExecutorB worker-{worker_id} executing priority={priority} task")
            task_func()
            self.task_queue.task_done()

    def submit_tasks(self, tasks: List[Tuple[int, Callable]]):
        """Submit tasks to the queue"""
        for task in tasks:
            self.task_queue.put(task)

    def terminate(self):
        """Terminate all workers"""
        print("TaskExecutorB terminate initiated")
        self.terminated = True
        
        # Send poison pills to all workers
        for _ in self.workers:
            self.task_queue.put((-float('inf'), None))
        
        # Wait for all workers to finish
        for worker in self.workers:
            worker.join()


if __name__ == "__main__":
    def do_task(p):
        time.sleep(1)
        print(f"Processing task with priority {p}")

    tasks = [(n, functools.partial(do_task, n)) for n in range(10)]

    # Test original TaskExecutor
    print("=== Testing Original TaskExecutor ===")
    executor = TaskExecutor(4)
    executor.submit_tasks(tasks)
    time.sleep(0.2)
    executor.submit_tasks([(-100, lambda: print("Top Priority Task Original"))])
    
    while True:
        time.sleep(0.1)
        if executor.capacity == 4:
            break
    
    executor.terminate()
    executor.loop_thread.join()

    print("\n=== Testing TaskExecutorB ===")
    executor_b = TaskExecutorB(4)
    executor_b.submit_tasks(tasks)
    time.sleep(0.2)
    executor_b.submit_tasks([(-100, lambda: print("Top Priority Task B"))])
    
    time.sleep(3)
    executor_b.terminate()
    
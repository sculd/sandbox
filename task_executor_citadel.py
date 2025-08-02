import threading
import heapq
import functools
import time


class TaskExecutor:
    def __init__(self, capacity):
        self.capacity = capacity
        self.tasks = []
        self.lock = threading.Lock()
        self.cond = threading.Condition()
        self.terminated = False
        self._loop()

    def _loop(self):
        def call_and_release(task_func):
            task_func()
            with self.lock:
                self.capacity += 1

            with self.cond:
                self.cond.notify_all()

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
                    threading.Thread(target=call_and_release, args=[task_func]).start()

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
            for task in tasks:
                heapq.heappush(self.tasks, task)
            #heapq.merge(self.tasks, tasks)

        with self.cond:            
            self.cond.notify_all()

    def terminate(self):
        print("terminate initiated")
        self.terminated = True

        with self.cond:            
            self.cond.notify_all()


if __name__ == "__main__":
    def do_task(p):
        time.sleep(1)
        print(f"Processing task with priority {p}")

    tasks = [(n, functools.partial(do_task, n)) for n in range(10)]

    executor = TaskExecutor(4)
    executor.submit_tasks(tasks)
    time.sleep(0.2)
    executor.submit_tasks([(-100, lambda: print("Top Priority Task"))])

    while True:
        time.sleep(0.1)
        if executor.capacity == 4:
            break

    executor.terminate()
    executor.loop_thread.join()

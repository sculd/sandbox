import threading
import multiprocessing
import time
import concurrent.futures
import requests
import math


# Root-level worker functions for multiprocessing
# These MUST be at module level to be pickle-able

def worker_wrapper_for_manual_processes(args):
    """Root-level worker function for manual process execution"""
    index, task, task_func, results_dict = args
    result = task_func(task)
    results_dict[index] = result


def worker_for_queue_based(task_queue, result_queue, task_func):
    """Root-level worker function for queue-based multiprocessing"""
    while True:
        item = task_queue.get()
        if item is None:  # Poison pill
            break
        
        index, task = item
        result = task_func(task)
        result_queue.put((index, result))


def count_to_million(*args):
    """Simple CPU task to show GIL impact"""
    count = 0
    for _ in range(20_000_000):
        count += 1
    return count

def cpu_intensive_task(n):
    """CPU-bound task: calculate prime numbers up to n"""
    def is_prime(num):
        if num < 2:
            return False
        for i in range(2, int(math.sqrt(num)) + 1):
            if num % i == 0:
                return False
        return True
    
    primes = [i for i in range(2, n) if is_prime(i)]
    return len(primes)


def io_intensive_task(url):
    """I/O-bound task: make HTTP request"""
    try:
        response = requests.get(url, timeout=5)
        return f"Status: {response.status_code}, Length: {len(response.content)}"
    except Exception as e:
        return f"Error: {str(e)}"


def sequential_execution(task_func, tasks, task_name):
    """Execute tasks sequentially"""
    print(f"\n=== Sequential {task_name} ===")
    start_time = time.time()
    
    results = []
    for i, task in enumerate(tasks):
        print(f"Processing task {i+1}/{len(tasks)}")
        result = task_func(task)
        results.append(result)
    
    end_time = time.time()
    print(f"Sequential execution took: {end_time - start_time:.2f} seconds")
    return results


def threading_execution(task_func, tasks, task_name):
    """Execute tasks using threading"""
    print(f"\n=== Threading {task_name} ===")
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(task_func, tasks))
    
    end_time = time.time()
    print(f"Threading execution took: {end_time - start_time:.2f} seconds")
    return results


def multiprocessing_execution(task_func, tasks, task_name):
    """Execute tasks using ProcessPoolExecutor"""
    print(f"\n=== ProcessPoolExecutor {task_name} ===")
    start_time = time.time()
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(task_func, tasks))
    
    end_time = time.time()
    print(f"ProcessPoolExecutor execution took: {end_time - start_time:.2f} seconds")
    return results


def multiprocessing_pool_execution(task_func, tasks, task_name):
    """Execute tasks using multiprocessing.Pool (older API)"""
    print(f"\n=== Multiprocessing Pool {task_name} ===")
    start_time = time.time()
    
    with multiprocessing.Pool(processes=4) as pool:
        results = pool.map(task_func, tasks)
    
    end_time = time.time()
    print(f"Multiprocessing Pool execution took: {end_time - start_time:.2f} seconds")
    return results


def manual_process_execution(task_func, tasks, task_name):
    """Execute tasks using manual Process creation"""
    print(f"\n=== Manual Process {task_name} ===")
    start_time = time.time()
    
    # Create a manager for sharing results
    manager = multiprocessing.Manager()
    results_dict = manager.dict()
    
    # Create and start processes
    processes = []
    batch_size = 4  # Process 4 tasks at a time
    
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        batch_processes = []
        
        for j, task in enumerate(batch):
            # Use root-level worker function
            p = multiprocessing.Process(
                target=worker_wrapper_for_manual_processes, 
                args=((i+j, task, task_func, results_dict),)
            )
            p.start()
            batch_processes.append(p)
        
        # Wait for this batch to complete
        for p in batch_processes:
            p.join()
        
        processes.extend(batch_processes)
    
    # Extract results in order
    results = [results_dict[i] for i in range(len(tasks))]
    
    end_time = time.time()
    print(f"Manual Process execution took: {end_time - start_time:.2f} seconds")
    return results


def queue_based_multiprocessing(task_func, tasks, task_name):
    """Execute tasks using Queue-based producer-consumer pattern"""
    print(f"\n=== Queue-based Multiprocessing {task_name} ===")
    start_time = time.time()
    
    # Create queues
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    
    # Fill task queue
    for i, task in enumerate(tasks):
        task_queue.put((i, task))
    
    # Add poison pills for workers
    for _ in range(4):
        task_queue.put(None)
    
    # Start worker processes using root-level function
    processes = []
    for _ in range(4):
        p = multiprocessing.Process(
            target=worker_for_queue_based, 
            args=(task_queue, result_queue, task_func)
        )
        p.start()
        processes.append(p)
    
    # Collect results
    results_dict = {}
    for _ in range(len(tasks)):
        index, result = result_queue.get()
        results_dict[index] = result
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
    
    # Extract results in order
    results = [results_dict[i] for i in range(len(tasks))]
    
    end_time = time.time()
    print(f"Queue-based multiprocessing took: {end_time - start_time:.2f} seconds")
    return results


def compare_cpu_intensive():
    """Compare threading vs multiprocessing for CPU-intensive tasks"""
    print("=" * 60)
    print("CPU-INTENSIVE TASK COMPARISON")
    print("Task: Count prime numbers up to N")
    print("=" * 60)
    
    # CPU-intensive tasks (prime counting) - much larger numbers for meaningful comparison
    cpu_tasks = [500000, 600000, 700000, 800000]
    
    # Sequential
    sequential_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    
    # Threading (will be limited by GIL)
    threading_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    
    # Different multiprocessing approaches
    multiprocessing_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    multiprocessing_pool_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    manual_process_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    queue_based_multiprocessing(cpu_intensive_task, cpu_tasks, "CPU Tasks")


def compare_io_intensive():
    """Compare threading vs multiprocessing for I/O-intensive tasks"""
    print("\n" + "=" * 60)
    print("I/O-INTENSIVE TASK COMPARISON")
    print("Task: HTTP requests to different URLs")
    print("=" * 60)
    
    # I/O-intensive tasks (HTTP requests)
    io_tasks = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1", 
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1"
    ]
    
    # Sequential
    sequential_execution(io_intensive_task, io_tasks, "I/O Tasks")
    
    # Threading (should be efficient for I/O-bound)
    threading_execution(io_intensive_task, io_tasks, "I/O Tasks")
    
    # Multiprocessing (higher overhead for I/O-bound)
    multiprocessing_execution(io_intensive_task, io_tasks, "I/O Tasks")


def main():
    """Main function to run all comparisons"""
    print("MULTI-THREADING vs MULTI-PROCESSING COMPARISON")
    print("Python version with GIL limitations")
    
    # Compare CPU-intensive tasks
    compare_cpu_intensive() 
    
    # Compare I/O-intensive tasks  
    compare_io_intensive()
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("• Threading: Good for I/O-bound tasks (network, file operations)")
    print("• Multiprocessing: Good for CPU-bound tasks (calculations, data processing)")
    print("• GIL prevents true parallelism in threading for CPU-bound tasks")
    print("• Multiprocessing has higher memory overhead but true parallelism")
    print("=" * 60)


if __name__ == "__main__":
    main()

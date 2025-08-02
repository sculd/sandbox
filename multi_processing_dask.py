import threading
import multiprocessing
import time
import concurrent.futures
import requests
import math
import logging
import dask
from dask import delayed
from dask.distributed import Client, as_completed
from dask.threaded import get as threaded_get
from dask.multiprocessing import get as multiprocessing_get

# Suppress Dask distributed logging
logging.getLogger('distributed').setLevel(logging.CRITICAL)
logging.getLogger('distributed.core').setLevel(logging.CRITICAL)
logging.getLogger('distributed.scheduler').setLevel(logging.CRITICAL)
logging.getLogger('distributed.worker').setLevel(logging.CRITICAL)
logging.getLogger('distributed.comm').setLevel(logging.CRITICAL)
logging.getLogger('distributed.client').setLevel(logging.CRITICAL)
logging.getLogger('distributed.nanny').setLevel(logging.CRITICAL)
logging.getLogger('tornado').setLevel(logging.CRITICAL)
from multi_processing import (
    cpu_intensive_task, 
    io_intensive_task, 
    sequential_execution, 
    threading_execution, 
    multiprocessing_execution,
    )



def dask_threaded_execution(task_func, tasks, task_name):
    """Execute tasks using Dask threaded scheduler"""
    print(f"\n=== Dask Threaded {task_name} ===")
    start_time = time.time()
    
    # Create delayed objects
    delayed_tasks = [delayed(task_func)(task) for task in tasks]
    
    # Execute using threaded scheduler
    results = dask.compute(*delayed_tasks, scheduler='threads', num_workers=4)
    
    end_time = time.time()
    print(f"Dask threaded execution took: {end_time - start_time:.2f} seconds")
    return results


def dask_multiprocessing_execution(task_func, tasks, task_name):
    """Execute tasks using Dask multiprocessing scheduler"""
    print(f"\n=== Dask Multiprocessing {task_name} ===")
    start_time = time.time()
    
    # Create delayed objects
    delayed_tasks = [delayed(task_func)(task) for task in tasks]
    
    # Execute using multiprocessing scheduler
    results = dask.compute(*delayed_tasks, scheduler='processes', num_workers=4)
    
    end_time = time.time()
    print(f"Dask multiprocessing execution took: {end_time - start_time:.2f} seconds")
    return results


def dask_distributed_execution(task_func, tasks, task_name):
    """Execute tasks using Dask distributed scheduler"""
    print(f"\n=== Dask Distributed {task_name} ===")
    start_time = time.time()
    
    try:
        # Start a local cluster with more conservative settings
        with Client(
            processes=True, 
            n_workers=2,  # Reduced from 4 to avoid resource contention
            threads_per_worker=2, 
            silence_logs=True,  # Reduce noise
            dashboard_address=None  # Disable dashboard to reduce overhead
        ) as client:
            print("Local Dask cluster started successfully")
            
            # Create delayed objects
            delayed_tasks = [delayed(task_func)(task) for task in tasks]
            
            # Execute using distributed scheduler
            futures = client.compute(delayed_tasks)
            results = client.gather(futures)
        
        end_time = time.time()
        print(f"Dask distributed execution took: {end_time - start_time:.2f} seconds")
        return results
        
    except Exception as e:
        end_time = time.time()
        print(f"Dask distributed execution failed: {str(e)}")
        print(f"Skipping distributed execution (took {end_time - start_time:.2f} seconds)")
        return [None] * len(tasks)


def compare_cpu_intensive():
    """Compare all methods for CPU-intensive tasks"""
    print("=" * 60)
    print("CPU-INTENSIVE TASK COMPARISON")
    print("Task: Count prime numbers up to N (Very Large Numbers)")
    print("=" * 60)
    
    # Very CPU-intensive tasks (prime counting) - matching user's intensive workload
    cpu_tasks = [500000, 600000, 700000, 800000]
    cpu_tasks = [t * 3 for t in cpu_tasks]
    
    # Sequential
    sequential_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    
    # Threading (will be limited by GIL)
    threading_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    
    # Multiprocessing
    multiprocessing_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    
    # Dask threaded (also limited by GIL)
    dask_threaded_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    
    # Dask multiprocessing
    dask_multiprocessing_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")
    
    # Dask distributed
    dask_distributed_execution(cpu_intensive_task, cpu_tasks, "CPU Tasks")


def compare_io_intensive():
    """Compare all methods for I/O-intensive tasks"""
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
    
    # Dask threaded (should be good for I/O)
    dask_threaded_execution(io_intensive_task, io_tasks, "I/O Tasks")
    
    # Dask multiprocessing (higher overhead)
    dask_multiprocessing_execution(io_intensive_task, io_tasks, "I/O Tasks")
    
    # Dask distributed (might have network overhead for simple I/O)
    dask_distributed_execution(io_intensive_task, io_tasks, "I/O Tasks")


def main():
    """Main function to run all comparisons"""
    print("DASK vs THREADING vs MULTIPROCESSING COMPARISON")
    print("Python with intensive workloads")
    
    # Compare CPU-intensive tasks
    compare_cpu_intensive() 
    
    # Compare I/O-intensive tasks  
    compare_io_intensive()
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("• Threading: Good for I/O-bound tasks (limited by GIL for CPU)")
    print("• Multiprocessing: Good for CPU-bound tasks (higher memory overhead)")
    print("• Dask Threaded: Similar to threading, good for I/O")
    print("• Dask Multiprocessing: Similar to multiprocessing, good for CPU")
    print("• Dask Distributed: Best for very large workloads, has cluster overhead")
    print("• Dask provides more flexibility and can scale to multiple machines")
    print("=" * 60)


if __name__ == "__main__":
    main()

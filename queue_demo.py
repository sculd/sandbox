import threading
import multiprocessing
import time
import os
import queue


# Root-level functions for multiprocessing (must be pickle-able)

def producer_process(q, n_items):
    """Producer process - generates data"""
    print(f"Producer (PID: {os.getpid()}) starting")
    for i in range(n_items):
        item = f"data_{i}"
        q.put(item)
        print(f"Producer put: {item}")
        time.sleep(0.1)
    
    # Signal end of data
    q.put(None)
    print("Producer finished")


def consumer_process(q, consumer_id):
    """Consumer process - processes data"""
    print(f"Consumer {consumer_id} (PID: {os.getpid()}) starting")
    while True:
        item = q.get()
        if item is None:
            # End signal received
            q.put(None)  # Pass signal to other consumers
            break
        
        print(f"Consumer {consumer_id} processing: {item}")
        time.sleep(0.2)  # Simulate processing
    
    print(f"Consumer {consumer_id} finished")


def thread_worker(thread_id, shared_queue, results_list):
    """Worker function for threading - shares memory directly"""
    print(f"Thread {thread_id} (PID: {os.getpid()}) starting")
    
    # Get item from shared queue
    item = shared_queue.get()
    print(f"Thread {thread_id} got: {item}")
    
    # Process the item
    result = item * 2
    
    # Store result in shared list (works because threads share memory)
    results_list.append(f"Thread {thread_id}: {result}")
    print(f"Thread {thread_id} finished")


def process_worker(process_id, shared_queue, result_queue):
    """Worker function for multiprocessing - separate memory space"""
    print(f"Process {process_id} (PID: {os.getpid()}) starting")
    
    # Get item from multiprocessing queue (IPC mechanism)
    item = shared_queue.get()
    print(f"Process {process_id} got: {item}")
    
    # Process the item
    result = item * 2
    
    # Can't use shared list! Must use another queue to send result back
    result_queue.put(f"Process {process_id}: {result}")
    print(f"Process {process_id} finished")


def demonstrate_threading_queue():
    """Show how threading.Queue works with shared memory"""
    print("=" * 60)
    print("THREADING QUEUE DEMONSTRATION (Shared Memory)")
    print("=" * 60)
    
    # Create shared objects (all threads see the same objects)
    shared_queue = queue.Queue()
    results_list = []  # Shared list - all threads can access
    
    print(f"Main thread PID: {os.getpid()}")
    print("Creating 3 threads...")
    
    # Put tasks in queue
    for i in range(3):
        shared_queue.put(i * 10)
    
    # Create threads
    threads = []
    for i in range(3):
        t = threading.Thread(target=thread_worker, args=(i, shared_queue, results_list))
        t.start()
        threads.append(t)
    
    # Wait for all threads
    for t in threads:
        t.join()
    
    print(f"Final results (shared list): {results_list}")
    print("✅ All threads shared the same memory space")


def demonstrate_multiprocessing_queue():
    """Show how multiprocessing.Queue works with separate memory spaces"""
    print("\n" + "=" * 60)
    print("MULTIPROCESSING QUEUE DEMONSTRATION (Separate Memory)")
    print("=" * 60)
    
    # Create multiprocessing queues (special IPC objects)
    shared_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    
    print(f"Main process PID: {os.getpid()}")
    print("Creating 3 processes...")
    
    # Put tasks in queue
    for i in range(3):
        shared_queue.put(i * 10)
    
    # Create processes
    processes = []
    for i in range(3):
        p = multiprocessing.Process(target=process_worker, args=(i, shared_queue, result_queue))
        p.start()
        processes.append(p)
    
    # Wait for all processes
    for p in processes:
        p.join()
    
    # Collect results from result queue
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    print(f"Final results (via result queue): {results}")
    print("✅ All processes had separate memory spaces")


def show_queue_internals():
    """Show what's happening under the hood with multiprocessing.Queue"""
    print("\n" + "=" * 60)
    print("MULTIPROCESSING QUEUE INTERNALS")
    print("=" * 60)
    
    q = multiprocessing.Queue()
    
    print("multiprocessing.Queue() creates:")
    print(f"   Type: {type(q)}")
    print(f"   Module: {q.__class__.__module__}")
    
    # Show some internal attributes (may vary by system)
    print(f"\nInternal mechanisms:")
    print(f"   _reader: {hasattr(q, '_reader')} (pipe for reading)")
    print(f"   _writer: {hasattr(q, '_writer')} (pipe for writing)")
    print(f"   _rlock: {hasattr(q, '_rlock')} (read lock)")
    print(f"   _wlock: {hasattr(q, '_wlock')} (write lock)")
    
    print(f"\nUnder the hood, multiprocessing.Queue uses:")
    print(f"   • Pipes or sockets for inter-process communication")
    print(f"   • Locks for thread-safe access within each process")
    print(f"   • Pickle/unpickle for data serialization across processes")
    print(f"   • Buffer management for efficient data transfer")


def demonstrate_serialization_overhead():
    """Show the serialization overhead of multiprocessing.Queue"""
    print("\n" + "=" * 60)
    print("SERIALIZATION OVERHEAD DEMONSTRATION")
    print("=" * 60)
    
    # Create both types of queues
    thread_queue = queue.Queue()
    process_queue = multiprocessing.Queue()
    
    # Large data to show serialization cost
    large_data = list(range(100000))  # 100k integers
    
    print(f"Data size: {len(large_data)} integers")
    
    # Threading queue (no serialization needed)
    start_time = time.time()
    thread_queue.put(large_data)
    retrieved = thread_queue.get()
    thread_time = time.time() - start_time
    
    print(f"Threading queue: {thread_time:.4f} seconds (direct memory access)")
    
    # Multiprocessing queue (serialization required)
    start_time = time.time()
    process_queue.put(large_data)
    retrieved = process_queue.get()
    process_time = time.time() - start_time
    
    print(f"Multiprocessing queue: {process_time:.4f} seconds (pickle + IPC)")
    print(f"Overhead factor: {process_time/thread_time:.1f}x slower")


def demonstrate_cross_process_communication():
    """Show a practical example of cross-process communication"""
    print("\n" + "=" * 60)
    print("PRACTICAL CROSS-PROCESS COMMUNICATION")
    print("=" * 60)
    
    # Create shared queue
    shared_queue = multiprocessing.Queue()
    
    # Start producer using root-level function
    producer = multiprocessing.Process(target=producer_process, args=(shared_queue, 5))
    producer.start()
    
    # Start consumers using root-level function
    consumers = []
    for i in range(2):
        consumer = multiprocessing.Process(target=consumer_process, args=(shared_queue, i))
        consumer.start()
        consumers.append(consumer)
    
    # Wait for all processes
    producer.join()
    for c in consumers:
        c.join()
    
    print("✅ Cross-process communication completed successfully")


if __name__ == "__main__":
    demonstrate_threading_queue()
    demonstrate_multiprocessing_queue()
    show_queue_internals()
    demonstrate_serialization_overhead()
    demonstrate_cross_process_communication() 
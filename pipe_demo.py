import multiprocessing
import time
import os
import threading
import queue


# Root-level functions for multiprocessing (pickle-able)

def pipe_sender_for_comparison(conn, n_messages):
    """Send messages through pipe for performance comparison"""
    for i in range(n_messages):
        conn.send(f"pipe_message_{i}")
    conn.send(None)
    conn.close()


def queue_sender_for_comparison(q, n_messages):
    """Send messages through queue for performance comparison"""
    for i in range(n_messages):
        q.put(f"queue_message_{i}")
    q.put(None)


def sender_that_closes_early(conn):
    """Worker that sends one message then closes (for error demo)"""
    conn.send("message1")
    conn.close()
    # Process exits, closing the pipe


def pipe_worker_send(conn, worker_id, data_list):
    """Worker that sends data through pipe"""
    print(f"Sender {worker_id} (PID: {os.getpid()}) starting")
    
    for i, data in enumerate(data_list):
        message = f"Worker {worker_id}: {data}"
        conn.send(message)
        print(f"Sender {worker_id} sent: {message}")
        time.sleep(0.1)
    
    # Send end signal
    conn.send(None)
    conn.close()
    print(f"Sender {worker_id} finished")


def pipe_worker_receive(conn, worker_id):
    """Worker that receives data through pipe"""
    print(f"Receiver {worker_id} (PID: {os.getpid()}) starting")
    
    while True:
        try:
            message = conn.recv()
            if message is None:
                break
            print(f"Receiver {worker_id} got: {message}")
            time.sleep(0.05)
        except EOFError:
            print(f"Receiver {worker_id}: Sender closed connection")
            break
    
    conn.close()
    print(f"Receiver {worker_id} finished")


def duplex_worker(conn, worker_id, messages):
    """Worker for duplex communication (can send and receive)"""
    print(f"Duplex worker {worker_id} (PID: {os.getpid()}) starting")
    
    # Send messages first
    for msg in messages:
        conn.send(f"From worker {worker_id}: {msg}")
        print(f"Worker {worker_id} sent: {msg}")
    
    # Signal we're done sending
    conn.send("WORKER_DONE_SENDING")
    
    # Now receive messages from parent
    try:
        while True:
            received = conn.recv()
            if received is None:
                print(f"Worker {worker_id} received end signal")
                break
            print(f"Worker {worker_id} received: {received}")
    except EOFError:
        print(f"Worker {worker_id}: Connection closed")
    
    conn.close()
    print(f"Duplex worker {worker_id} finished")


def demonstrate_basic_pipe():
    """Show basic pipe communication between two processes"""
    print("=" * 60)
    print("BASIC PIPE DEMONSTRATION")
    print("=" * 60)
    
    # Create a pipe (returns two Connection objects)
    read_conn, write_conn = multiprocessing.Pipe()
    
    print("multiprocessing.Pipe() creates two Connection objects:")
    print(f"  Read connection: {type(read_conn)}")
    print(f"  Write connection: {type(write_conn)}")
    print(f"  Read conn readable: {read_conn.readable}")
    print(f"  Read conn writable: {read_conn.writable}")
    
    # Start child process
    child_process = multiprocessing.Process(
        target=pipe_worker_send, 
        args=(write_conn, 1, ["data1", "data2", "data3"])
    )
    child_process.start()
    
    # Parent receives data
    print(f"\nParent (PID: {os.getpid()}) receiving data...")
    while True:
        try:
            message = read_conn.recv()
            if message is None:
                break
            print(f"Parent received: {message}")
        except EOFError:
            print("Child closed the connection")
            break
    
    read_conn.close()
    child_process.join()
    print("✅ Basic pipe communication completed")


def demonstrate_duplex_pipe():
    """Show duplex (bidirectional) pipe communication"""
    print("\n" + "=" * 60)
    print("DUPLEX PIPE DEMONSTRATION")
    print("=" * 60)
    
    # Create duplex pipe (default is duplex=True)
    read_conn, write_conn = multiprocessing.Pipe(duplex=True)
    
    print("Duplex pipe allows bidirectional communication")
    
    # Start child process
    child_process = multiprocessing.Process(
        target=duplex_worker,
        args=(write_conn, "Child", ["hello", "from", "child"])
    )
    child_process.start()
    
    # Parent receives child's messages first (avoid deadlock)
    print(f"Parent (PID: {os.getpid()}) starting duplex communication")
    
    # First, receive all messages from child
    print("Parent receiving messages from child...")
    try:
        while True:
            received = read_conn.recv()
            if received == "WORKER_DONE_SENDING":
                print("Parent: Child finished sending messages")
                break
            print(f"Parent received: {received}")
    except EOFError:
        print("Child closed connection unexpectedly")
    
    # Now parent sends messages to child
    print("Parent sending messages to child...")
    parent_messages = ["hello", "from", "parent"]
    for msg in parent_messages:
        read_conn.send(f"From parent: {msg}")
        print(f"Parent sent: {msg}")
    
    # Signal end to child
    read_conn.send(None)
    print("Parent sent end signal")
    
    read_conn.close()
    child_process.join()
    print("✅ Duplex pipe communication completed")


def demonstrate_simplex_pipe():
    """Show simplex (unidirectional) pipe communication"""
    print("\n" + "=" * 60)
    print("SIMPLEX PIPE DEMONSTRATION")
    print("=" * 60)
    
    # Create simplex pipe (unidirectional)
    read_conn, write_conn = multiprocessing.Pipe(duplex=False)
    
    print("Simplex pipe: read_conn can only receive, write_conn can only send")
    print(f"  read_conn readable: {read_conn.readable}, writable: {read_conn.writable}")
    print(f"  write_conn readable: {write_conn.readable}, writable: {write_conn.writable}")
    
    # Start sender process
    sender_process = multiprocessing.Process(
        target=pipe_worker_send,
        args=(write_conn, "Sender", ["simplex1", "simplex2", "simplex3"])
    )
    sender_process.start()
    
    # Parent receives (read_conn is read-only)
    print(f"\nParent (PID: {os.getpid()}) receiving from simplex pipe...")
    while True:
        try:
            message = read_conn.recv()
            if message is None:
                break
            print(f"Parent received: {message}")
        except EOFError:
            break
    
    read_conn.close()
    sender_process.join()
    print("✅ Simplex pipe communication completed")


def compare_pipe_vs_queue():
    """Compare performance and characteristics of Pipe vs Queue"""
    print("\n" + "=" * 60)
    print("PIPE vs QUEUE COMPARISON")
    print("=" * 60)
    
    n_messages = 1000
    
    # Test Pipe performance
    print(f"Sending {n_messages} messages...")
    
    read_conn, write_conn = multiprocessing.Pipe()
    start_time = time.time()
    
    sender_process = multiprocessing.Process(target=pipe_sender_for_comparison, args=(write_conn, n_messages))
    sender_process.start()
    
    received_count = 0
    while True:
        message = read_conn.recv()
        if message is None:
            break
        received_count += 1
    
    pipe_time = time.time() - start_time
    read_conn.close()
    sender_process.join()
    
    # Test Queue performance
    q = multiprocessing.Queue()
    start_time = time.time()
    
    sender_process = multiprocessing.Process(target=queue_sender_for_comparison, args=(q, n_messages))
    sender_process.start()
    
    received_count = 0
    while True:
        message = q.get()
        if message is None:
            break
        received_count += 1
    
    queue_time = time.time() - start_time
    sender_process.join()
    
    print(f"\nPerformance Results:")
    print(f"  Pipe time: {pipe_time:.4f} seconds")
    print(f"  Queue time: {queue_time:.4f} seconds")
    print(f"  Pipe is {queue_time/pipe_time:.1f}x faster than Queue")
    
    print(f"\nCharacteristics:")
    print(f"  Pipe:")
    print(f"    • Point-to-point (2 processes only)")
    print(f"    • Lower overhead")
    print(f"    • Direct connection")
    print(f"    • No built-in synchronization for multiple readers/writers")
    print(f"  Queue:")
    print(f"    • Many-to-many (multiple processes)")
    print(f"    • Higher overhead")
    print(f"    • Thread-safe operations")
    print(f"    • Built-in synchronization")


def demonstrate_pipe_internals():
    """Show what's happening under the hood with pipes"""
    print("\n" + "=" * 60)
    print("PIPE INTERNALS DEMONSTRATION")
    print("=" * 60)
    
    read_conn, write_conn = multiprocessing.Pipe()
    
    print("multiprocessing.Pipe() creates Connection objects:")
    print(f"  Type: {type(read_conn)}")
    print(f"  Module: {read_conn.__class__.__module__}")
    
    # Show internal attributes
    print(f"\nConnection object attributes:")
    print(f"  readable: {read_conn.readable}")
    print(f"  writable: {read_conn.writable}")
    print(f"  closed: {read_conn.closed}")
    
    # Show file descriptor (Unix systems)
    if hasattr(read_conn, 'fileno'):
        try:
            print(f"  file descriptor: {read_conn.fileno()}")
        except:
            print(f"  file descriptor: Not available")
    
    print(f"\nUnder the hood, Pipe uses:")
    print(f"  • OS pipes (Unix) or named pipes (Windows)")
    print(f"  • File descriptors for communication")
    print(f"  • Kernel buffers for data transfer")
    print(f"  • Pickle for object serialization")
    print(f"  • No additional locking (unlike Queue)")
    
    read_conn.close()
    write_conn.close()


def demonstrate_pipe_errors():
    """Show common pipe errors and edge cases"""
    print("\n" + "=" * 60)
    print("PIPE ERROR HANDLING DEMONSTRATION")
    print("=" * 60)
    
    read_conn, write_conn = multiprocessing.Pipe()
    
    # Start process that closes early using root-level function
    process = multiprocessing.Process(target=sender_that_closes_early, args=(write_conn,))
    process.start()
    
    print("Demonstrating EOFError when sender closes:")
    
    # Receive the first message
    msg1 = read_conn.recv()
    print(f"Received: {msg1}")
    
    # Wait for process to fully terminate
    process.join()
    print("Sender process has terminated")
    read_conn.close()
    
    # Now try to read again - this should raise EOFError
    print("Attempting to read from closed pipe...")
    try:
        # Give a small delay to ensure OS cleanup
        time.sleep(0.1)
        msg2 = read_conn.recv()
        print(f"Unexpectedly received: {msg2}")
    except EOFError as e:
        print(f"✅ Caught EOFError as expected: {e}")
        print("This happens when trying to read from a closed pipe")
    except Exception as e:
        print(f"✅ Caught: {e}")
        print("This happens when trying to read from a closed pipe")
    
    # Demonstrate writing to closed pipe
    print(f"\nDemonstrating BrokenPipeError:")
    read_conn, write_conn = multiprocessing.Pipe()
    read_conn.close()  # Close receiving end
    
    try:
        write_conn.send("This will fail")
    except BrokenPipeError as e:
        print(f"Caught BrokenPipeError: {e}")
        print("This happens when trying to write to a closed pipe")
    except OSError as e:
        print(f"Caught OSError: {e}")
        print("On some systems, this might be OSError instead")
    
    write_conn.close()


if __name__ == "__main__":
    demonstrate_basic_pipe()
    demonstrate_duplex_pipe()
    demonstrate_simplex_pipe()
    compare_pipe_vs_queue()
    demonstrate_pipe_internals()
    demonstrate_pipe_errors() 
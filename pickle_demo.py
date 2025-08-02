import pickle
import dis


def root_level_function():
    """This function can be pickled because it's at module level"""
    return "I'm at the root level!"


def demonstrate_function_pickling():
    """Demonstrate what happens when pickling functions"""
    
    print("=== FUNCTION PICKLING DEMONSTRATION ===\n")
    
    # 1. Show that functions are objects
    print("1. Functions are objects:")
    print(f"   Type: {type(root_level_function)}")
    print(f"   Name: {root_level_function.__name__}")
    print(f"   Module: {root_level_function.__module__}")
    print(f"   Memory address: {id(root_level_function)}")
    
    # 2. Pickle the function
    print("\n2. Pickling the function:")
    pickled_func = pickle.dumps(root_level_function)
    print(f"   Pickled size: {len(pickled_func)} bytes")
    print(f"   Pickled data: {pickled_func}")
    
    # 3. Show what's in the pickle
    print("\n3. What's actually in the pickle:")
    # The pickle contains module name and function name, not the code
    print("   The pickle contains:")
    print("   - Module name: '__main__'") 
    print("   - Function name: 'root_level_function'")
    print("   - NOT the actual function code!")
    
    # 4. Unpickle it
    print("\n4. Unpickling the function:")
    unpickled_func = pickle.loads(pickled_func)
    print(f"   Unpickled function: {unpickled_func}")
    print(f"   Same memory address? {id(unpickled_func) == id(root_level_function)}")
    print(f"   Same function? {unpickled_func is root_level_function}")
    print(f"   Result: {unpickled_func()}")
    
    # 5. Show function bytecode (the actual "code")
    print("\n5. The actual function code (bytecode):")
    print("   This is what would need to be serialized to truly 'pickle' a function:")
    dis.dis(root_level_function)


def demonstrate_local_function_failure():
    """Show why local functions can't be pickled"""
    
    print("\n=== LOCAL FUNCTION PICKLE FAILURE ===\n")
    
    def local_function():
        """This local function cannot be pickled"""
        return "I'm local and can't be pickled!"
    
    print("1. Trying to pickle a local function:")
    print(f"   Local function: {local_function}")
    print(f"   Module: {local_function.__module__}")
    print(f"   Qualified name: {local_function.__qualname__}")
    
    try:
        pickled = pickle.dumps(local_function)
        print("   ✅ Somehow it worked?!")
    except Exception as e:
        print(f"   ❌ Failed as expected: {type(e).__name__}: {e}")
        print("   Why? No importable path: 'demonstrate_local_function_failure.<locals>.local_function'")


def show_multiprocessing_context():
    """Show what happens in multiprocessing context"""
    
    print("\n=== MULTIPROCESSING CONTEXT ===\n")
    
    print("When you create a process:")
    print("1. multiprocessing.Process(target=my_function)")
    print("2. Python pickles 'my_function' to send to new process")
    print("3. New process unpickles: 'from __main__ import my_function'")
    print("4. New process executes the function")
    print()
    print("This is why the function must be importable!")


if __name__ == "__main__":
    demonstrate_function_pickling()
    demonstrate_local_function_failure()
    show_multiprocessing_context() 
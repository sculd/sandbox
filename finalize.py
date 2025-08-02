import gc
import weakref

class WithDel:
    def __init__(self, name):
        self.name = name
        self.other = None
    def __del__(self):
        print(f"__del__ called for {self.name}")

class Bad:
    def __del__(self):
        print("Bad.__del__ called")
        print(f"Trying to access other: {self.other}")

class WithFinalize:
    def __init__(self, name):
        self.name = name
        self.other = None
        weakref.finalize(self, WithFinalize.cleanup, name)

    @staticmethod
    def cleanup(name):
        print(f"finalize called for {name}")

def test_with_del():
    print("\n=== Test: __del__ + cyclic ref ===")
    print(f"Before gc.garbage: {gc.garbage}")
    gc.collect()
    gc.garbage.clear()
    gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

    a = WithDel("a")
    b = WithDel("b")
    a.other = b
    b.other = a

    del a, b
    collected = gc.collect()
    print(f"GC collected {collected} objects")
    print(f"After gc.garbage: {gc.garbage}")  # should contain uncollectable cycle

def test_bad_cycle():
    print("\n=== Test: Bad __del__ + cyclic ref with dependency ===")
    print(f"Before gc.garbage: {gc.garbage}")
    gc.collect()
    gc.garbage.clear()
    gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

    a = Bad()
    b = Bad()
    a.other = b
    b.other = a

    del a, b
    collected = gc.collect()
    print(f"GC collected {collected} objects")
    print(f"After gc.garbage: {gc.garbage}")

def test_with_finalize():
    print("\n=== Test: weakref.finalize + cyclic ref ===")
    print(f"Before gc.garbage: {gc.garbage}")
    gc.collect()
    gc.garbage.clear()
    gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

    a = WithFinalize("x")
    b = WithFinalize("y")
    a.other = b
    b.other = a

    del a, b
    collected = gc.collect()
    print(f"GC collected {collected} objects")
    print(f"After gc.garbage: {gc.garbage}")  # should be empty

if __name__ == "__main__":
    test_with_del()
    test_bad_cycle()
    test_with_finalize()


import cProfile
cProfile.run('test_with_del()')

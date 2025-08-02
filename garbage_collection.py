import gc
print(gc.get_stats())

class A:
    pass

class B:
    pass

a, b = A(), B()

a.b = b
b.a = a


del a
del b

print(gc.garbage)
c = gc.collect()
print(c, gc.garbage)
print(gc.get_stats())


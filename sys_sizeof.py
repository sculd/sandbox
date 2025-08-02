import sys
print(sys.getsizeof(42))
print(sys.getsizeof(10**10))
print(sys.getsizeof(10**20))
print(sys.getsizeof(10**100+1))
print(sys.getsizeof("hello"))

print(sys.getsizeof([i for i in range(3)]))
print(sys.getsizeof([i for i in range(100)]))

print(0.1 + 0.2 == 0.3)  # False
print(0.1 + 0.2)

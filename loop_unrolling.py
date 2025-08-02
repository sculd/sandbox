# @jit(nopython=True)
def func1(array):
    numSum = 0

    for i in range(array.shape[0]):
        numSum += array[i]

    return numSum

# 2x2 unrolled
# @jit(nopython=True)
def unrolledFunc1(array):
    numSum0 = 0
    numSum1 = 0 

    for i in range(0, array.shape[0]-1, 2):
        numSum0 += array[i] 
        numSum1 += array[i+1] 

    numSum = numSum0 + numSum1 

    return numSum

import numpy as np
vs = np.random.rand(10 ** 7)

import time
t1 = time.perf_counter()
for _ in range(10):
    func1(vs)
t2 = time.perf_counter()
print(f"func1: {t2 - t1:.6f}")

t1 = time.perf_counter()
for _ in range(10):
    unrolledFunc1(vs)
t2 = time.perf_counter()
print(f"unrolledFunc1: {t2 - t1:.6f}")

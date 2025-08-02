import contextlib
import time
import random


@contextlib.contextmanager
def measure_time(label):
    t1 = time.perf_counter()
    try:
        yield
    except TypeError as te:
        print(f"{te}")
    finally:
        print(f"{label} measured: {time.perf_counter() - t1:.3f}")


with measure_time("random_adds"):
    tot = 0
    for i in range(10 ** 7):
        tot += random.random()

        if i > 10 ** 5:
            raise TypeError(f"type error at {i=}")

        if i > 10 ** 6:
            raise ValueError(f"value error at {i=}")

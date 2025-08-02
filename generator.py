
def count_up_to(n):
    i = 0
    while i < n:
        yield i
        i += 1

print([i for i in count_up_to(10)])



def example():
    yield 1
    yield 2
    # this value is stored in StopIteration
    return 100

print([i for i in example()])





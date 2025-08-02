
class MyContext:
    def __init__(self, filename):
        self.f = open(filename, "wt")

    def __enter__(self):
        return self.f

    def __exit__(self, etype, value, trace):
        self.f.close()

    def show(self, v):
        print(v)

with MyContext("data/dummy1.txt") as c:
    c.write("abc")


from contextlib import contextmanager

@contextmanager
def my_context(filename):
    try:
        f = open(filename, "wt")
        yield f
    except:
        pass
    finally:
        f.close()

with my_context("data/dummy2.txt") as c:
    c.write("123")


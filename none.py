class FakeNone:
    def __eq__(self, other):
        print("FakeNone.__eq__ called")

        return True

f = FakeNone()
#print(f == None)    # True
print(None == f)    # True, because f.__eq__ is used
print(f is None)    # False, 정확히 None이 아님

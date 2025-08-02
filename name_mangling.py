class Base:
    def __init__(self):
        self.__x = 10  # becomes _Base__x
        self._y = 10
        self.z = 10

    def show(self):
        print(f"x: {self.__x}, y: {self._y}")

class Derived(Base):
    def __init__(self):
        super().__init__()
        self.__x = 99  # becomes _Derived__x (different!)
        self._y = 99
        self.z = 99

d = Derived()
d.show()  # prints 10, not 99

print(f"_Base__x: {d._Base__x}, _Derived__x: {d._Derived__x}")


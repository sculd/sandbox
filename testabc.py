from abc import ABC, abstractmethod

class MyBaseClass(ABC):
    @abstractmethod
    def do_work(self):
        pass

class Concrete(MyBaseClass):
    def do_work(self):
        print("do_work")

class AbstractBaseClass:
    @abstractmethod
    def do_work(self):
        pass

class IsConcrete(AbstractBaseClass):
    pass

c = Concrete()
c.do_work()

ic = IsConcrete()


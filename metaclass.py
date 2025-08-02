class MyMeta(type):
    def __new__(cls, name, bases, dct):
        print(f"[metaclass] class: {cls}")
        print(f"[metaclass] class name: {name}")
        print(f"[metaclass] bases: {bases}")
        print(f"[metaclass] attributes: {list(dct.keys())}")
        return super().__new__(cls, name, bases, dct)

class Sample(metaclass=MyMeta):
    value = 1

    def method(self):
        return "Hello"


class ChildSample(Sample):
    pass


class InterfaceChecker(type):
    def __new__(cls, name, bases, dct):
        if bases != () and 'run' not in dct:
            raise TypeError(f"{name} class must define run() method.")
        return super().__new__(cls, name, bases, dct)

class Base(metaclass=InterfaceChecker):
    pass

class Good(Base):
    def run(self):
        pass

registry = {}

class AutoRegister(type):
    def __new__(cls, name, bases, dct):
        new_cls = super().__new__(cls, name, bases, dct)
        registry[name] = new_cls
        return new_cls

class PluginBase(metaclass=AutoRegister):
    pass

class Plugin(PluginBase):
    pass

print(registry)  # {'PluginBase': ..., 'Plugin': ...}


my_registry = {}

class MyRegister(type):
    registry = {}
    def __new__(cls, name, bases, dct):
        print(f"{cls=}, {name=}")
        c = super().__new__(cls, name, bases, dct)
        my_registry[name] = c
        cls.registry[name] = c
        return c


class MyBase(metaclass=MyRegister):
    pass

class MyPlugin(MyBase):
    pass

print(my_registry)  # {'PluginBase': ..., 'MyPlugin': ...}
print(MyRegister.registry)
print(MyBase.registry)
print(MyPlugin.registry)


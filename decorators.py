
def func_deco(func):
    def wrapper(*args, **kargs):
        print(f"with func decorator, running {func.__name__} with {args}, {kargs}")
        return func(*args, **kargs)
    return wrapper


@func_deco
def myfunc(v):
    return v


print(myfunc(123))


def func_deco_factory(label):
    def _deco(func):
        def wrapper(*args, **kargs):
            print(f"with function decorator of {label}, {func.__name__} is called with {args}, {kargs}")
            return func(*args, **kargs)
        return wrapper
    return _deco


@func_deco_factory("myfunc2_label")
def myfunc2(v):
    return v


print(myfunc2(123))



def cls_deco(cls):
    def __repr__(self):
        return f"{cls.__name__}()" 
    cls.__repr__ = __repr__
    return cls

@cls_deco
class mycls:
    def __init__(self):
        print(f"{self!s} instance created")

    def __str__(self):
        return f"mycls"

    def echo(self, v):
        return v


c = mycls()
print(f"{c!r}, {c.echo(321)}" )


def cls_deco_factory(label):
    def cls_deco(cls):
        def __repr__(self):
            return f"{cls.__name__}()" 
        cls.__repr__ = __repr__
        def __str__(self):
            return f"{cls.__name__} with label '{label}'" 
        cls.__str__ = __str__
        return cls
        
    return cls_deco


@cls_deco_factory("mycls2_label")
class mycls2:
    def __init__(self):
        print(f"{self!s} instance created")

    def __str__(self):
        return f"mycls2"

    def echo(self, v):
        return v


c = mycls2()
print(f"{c!r}, {c.echo(321)}" )

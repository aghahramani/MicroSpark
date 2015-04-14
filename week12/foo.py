
class Foo(object):
    def __init__(self, x, func):
        self.x = x
        self.func = func

    def apply(self):
        self.x = self.func(self.x)

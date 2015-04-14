import pickle
import StringIO

import zerorpc
import cloudpickle

import foo

y = 100
f = foo.Foo(99, lambda x: y + x + 1)

print('f.x = ' + str(f.x))

output = StringIO.StringIO()
pickler = cloudpickle.CloudPickler(output)
pickler.dump(f)
objstr = output.getvalue()


c = zerorpc.Client()
c.connect("tcp://127.0.0.1:4242")

print c.hello(objstr)

y = 10000
#f = foo.Foo(99, lambda x: y + x + 1)

print('f.x = ' + str(f.x))

output = StringIO.StringIO()
pickler = cloudpickle.CloudPickler(output)
pickler.dump(f)
objstr = output.getvalue()

print c.hello(objstr)



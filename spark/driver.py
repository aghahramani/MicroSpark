#!/usr/bin/python
import pickle
import StringIO

import zerorpc
import cloudpickle

from rdd import *
from os import listdir
from os.path import isfile, join
port = 4242
count = 0
mypath = './Data/'
files = [ join(mypath,f) for f in listdir(mypath) if isfile(join(mypath,f)) ]
for file in files:
    r = TextFile(file)
    m = Map(r, lambda s: s.split())
    m.set_persist()
    fm = FlatMap(m , lambda a : a)
    f = Filter(fm, lambda a: type(a) == str)

    output = StringIO.StringIO()
    pickler = cloudpickle.CloudPickler(output)
    pickler.dump(fm)
    objstr = output.getvalue()

    c = zerorpc.Client()
    c.connect("tcp://127.0.0.1:"+str(port+count))
    count+=1

    print c.hello(objstr)


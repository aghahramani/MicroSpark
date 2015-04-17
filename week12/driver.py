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
    f = Filter(m, lambda a: int(a[1]) > 2)

    output = StringIO.StringIO()
    pickler = cloudpickle.CloudPickler(output)
    pickler.dump(f)
    objstr = output.getvalue()

    c = zerorpc.Client()
    c.connect("tcp://127.0.0.1:"+str(port+count))
    count+=1

    print c.hello(objstr)


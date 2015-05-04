#!/usr/bin/python
import StringIO
import pickle
import sys
import cloudpickle
import gevent
import zerorpc

import rdd

class Worker(object):

    def __init__(self):
        #gevent.spawn(self.controller)
        self.hash_range = None
        self.f = None
        pass

    def controller(self):
        while True:
            print "[Contoller]"
            gevent.sleep(1)

    def hello(self, objstr):
        input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(input)
        self.f = unpickler.load()
        self.f.set_id(sys.argv[1])
        self.f.set_id_range(int(sys.argv[2]),int(sys.argv[3])+1)
        self.hash_range = int(sys.argv[3])+1 - int(sys.argv[2])
        return self.f.collect()


    def get_data(self,port,height,hash_func,fetch_all = False):
        input = StringIO.StringIO(hash_func)
        unpickler = pickle.Unpickler(input)
        hash_func= unpickler.load()
        temp = []

        if self.f :
            for i in self.f.get_data(height) :
                if i  == None :
                    continue
                if not fetch_all:
                    if i :
                        if hash_func(i[0]) == port:
                            temp.append(i)
                else:
                    if i :
                        temp.append(i)
        return temp



s = zerorpc.Server(Worker())
s.bind("tcp://0.0.0.0:"+ sys.argv[1])
s.run()

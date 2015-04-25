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
        return str(self.f.collect())


    def get_data(self,port):
        if self.f == None :
            return None
        data = self.f.get_data()
        if not data :
            return None
        temp = []
        for i in data :
            if ((hash(i)%self.hash_range) + int(sys.argv[1])) == port:
                temp.append([i , data[i]])
        return temp



s = zerorpc.Server(Worker())
s.bind("tcp://0.0.0.0:"+ sys.argv[1])
s.run()

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
        self.data_book={}
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
        return self.f.collect()


    def get_data(self,port,height,hash_func,fetch_all = False, forced = False):
        input = StringIO.StringIO(hash_func)
        unpickler = pickle.Unpickler(input)
        hash_func= unpickler.load()
        temp = []
        while self.f == None : # Reason : When we call hello method on each worker there is a chance that one of the
            #workers not yet initialized but the other worker calls get_data on it. This is why we need this
            gevent.sleep(0.001)
        #print self.f,self.f.height,len(self.f.get_dependencies()),"-------------------------------"
        for i in self.f.get_data(height) :
            if i  == None :
                continue
            if not fetch_all:
                if i :
                    if hash_func(i[0]) == port[0]:
                        temp.append(i)
            else:
                if i :
                    temp.append(i)
        return temp


s = zerorpc.Server(Worker())
s.bind("tcp://0.0.0.0:"+ sys.argv[1])
s.run()

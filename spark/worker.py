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
        self.f.set_id_range(int(sys.argv[2]),int(sys.argv[3]))
        self.hash_range = int(sys.argv[3])+1 - int(sys.argv[2])
        return self.f.collect()


    def get_data(self,port,height,hash_func,fetch_all = False, forced = False):
        input = StringIO.StringIO(hash_func)
        unpickler = pickle.Unpickler(input)
        hash_func= unpickler.load()
        temp = []
        # if height not in self.data_book:
        #     self.data_book[height] = []
        # if (port[0] not in self.data_book[height] )or forced:
            #self.data_book[height].append(port[0])
        if self.f :
            #print self.f.get_id(),port
            for i in self.f.get_data(height) :
                if i  == None :
                    #gevent.sleep(0.001)
                    continue
                if not fetch_all:
                    if i :
                        if hash_func(i[0]) == port[0]:
                            temp.append(i)
                else:
                    if i :
                        temp.append(i)
            #if ['10','1'] in temp:
            #    print temp
            #       print self.f.get_id() , port
        return temp



s = zerorpc.Server(Worker())
s.bind("tcp://0.0.0.0:"+ sys.argv[1])
s.run()

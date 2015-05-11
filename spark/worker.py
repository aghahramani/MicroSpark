#!/usr/bin/python
import StringIO
import argparse
import pickle
import sys
import cloudpickle
import gevent
import zerorpc
import rdd

#for logging zerorpc errors
import logging
logging.basicConfig()

class Worker(object):

    def __init__(self):
        #gevent.spawn(self.controller)
        self.hash_range = None
        self.f = None
        self.data_book={}
        pass

    def ping(self):
        return True

    def controller(self):
        while True:
            print "[Contoller]"
            gevent.sleep(1)

    def hello_with_failure(self,objstr):
        if args.port == '4244' or args.port == '4243':
            s.close()
        else:
            input = StringIO.StringIO(objstr)
            unpickler = pickle.Unpickler(input)
            self.f = unpickler.load()
            self.f.set_id(args.port)
            return self.f.collect()

    def hello(self, objstr):
        input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(input)
        self.f = unpickler.load()
        self.f.set_id(args.port)
        return self.f.collect()

    def get_data_async(self,port,height,hash_func,fetch_all = False, forced = False):
        input = StringIO.StringIO(hash_func)
        unpickler = pickle.Unpickler(input)
        hash_func= unpickler.load()
        temp = []
        while self.f == None : # Reason : When we call hello method on each worker there is a chance that one of the
            #workers not yet initialized but the other worker calls get_data on it. This is why we need this
            gevent.sleep(0.001)
        #print self.f,self.f.height,len(self.f.get_dependencies()),"-------------------------------"
        #print "Asking from ", port, "I am ",self.f.get_id(), "Height is" , height
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
        #print "returning len of ", len(temp)
        return temp


    def get_data(self,port,height,hash_func,fetch_all = False, forced = False):
        g = gevent.spawn(self.get_data_async,port,height,hash_func,fetch_all,forced)
        g.join()
        return g.value


if __name__ == '__main__':

    parse=argparse.ArgumentParser()
    parse.add_argument("--ec2", action="store_true")
    parse.add_argument("port", help="port",type=int)
    args=parse.parse_args()
    s = zerorpc.Server(Worker())
    if (args.ec2):
        s.bind("tcp://0.0.0.0:"+ str(args.port))
    else:
        s.bind("tcp://127.0.0.1:"+ str(args.port))
    s.run()

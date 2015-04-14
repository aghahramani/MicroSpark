import StringIO
import pickle

import cloudpickle
import gevent
import zerorpc

class Worker(object):

    def __init__(self):
        #gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print "[Contoller]"
            gevent.sleep(1)

    def hello(self, objstr):
        input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(input)
        f = unpickler.load()
        f.apply()

        print "[Request] f.x = ", f.x
        return "ACK"

s = zerorpc.Server(Worker())
s.bind("tcp://0.0.0.0:4242")
s.run()

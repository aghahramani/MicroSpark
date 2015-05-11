#!/usr/bin/python
import pickle
import StringIO
import argparse
import zerorpc
import EC2Worker
import cloudpickle

from rdd import *
from os import listdir
import subprocess
from os import system
from os.path import isfile, join
import sys
from gevent import Greenlet


LOCALHOST="127.0.0.1"

class WorkerQueue(object):

    g = None

    def __init__(self, n = 40 ):
        self.init_ip = 4242
        self.workers = [self.start_worker(self.init_ip+_) for _ in xrange(n)]
        self.m = Master()
        WorkerQueue.g = gevent.spawn(self.start_server,self.m)
        self.gevent_list=[]
        self.n = 20

    def connect(self,count):
        c = zerorpc.Client()
        c.connect("tcp://"+LOCALHOST+":"+str(count))
        return c

    def start_job(self,count,ob):
        c=self.connect(count)
        ttt = c.hello(ob)
        c.close()
        return ttt

    def start_job_fail_test(self,count,ob):
        c=self.connect(count)
        ttt = c.hello_with_failure(ob)
        c.close()
        return ttt


    def start_server(self,m):
        s = zerorpc.Server(m)
        s.bind("tcp://"+LOCALHOST+":4241")
        s.run()

    def start_worker(self,port):
        #if port != 4247:
        system("./worker.py " +str(port) + " &" )
        return port

    def get_worker(self):
        if len(self.workers)==0:
            self.workers.append(self.start_worker(self.init_ip+self.n))
            self.n +=1
        return self.workers.pop(0)

    def test(self):
        return "test successful"

    def get_g_list(self):
        print self.gevent_list
        return self.gevent_list

    def get_init_ip(self):
        return self.init_ip

    def add_failed_nodes(self,value):
        geven_lis = []
        for i in value:
            if i not in self.failed_nodes:
                self.failed_nodes[i] = True
                geven_lis.append(gevent.spawn(self.ping,i))
        if len(geven_lis) > 0 :
            gevent.joinall(geven_lis)
            for i_index, i in enumerate(geven_lis):
                if i.value == None:
                    system("./worker.py " +str(value[i_index]) + " force &" )
                    # g = self.gevent_list[value[i_index]-self.init_ip].start()
                    # self.gevent_list[value[i_index]-self.init_ip] = g
                    # self.gevent_list[value[i_index]-self.init_ip].join()
                    # print g.value()
                    self.gevent_list[value[i_index]-self.init_ip]=\
                    gevent.spawn(self.start_job,value[i_index],Parallel.para_worker_dict[value[i_index]])




    def ping(self,value):
        c = self.create_connection(value)
        if c.ping():
            c.close()
            if value in self.failed_nodes:
                del self.failed_nodes[value]
            return True
        c.close()

    def create_connection(self,value):
        c = zerorpc.Client(timeout=5)
        c.connect("tcp://"+LOCALHOST+":" + str(value))
        return c


class Master(WorkerQueue):

    def __init__(self):
        self.failed_nodes = {}
        self.gevent_list = []
        self.init_ip = 4242
        pass





class Parallel(object):

    para_worker_dict={}

    def __init__(self,worker_queue):
        self.wq = worker_queue
        self.dependencies=[]
        self.gevent_list = []
        self.fail_test = False

    def textFile(self,mypath):
        files = [ join(mypath,f) for f in listdir(mypath) if isfile(join(mypath,f))]
        text_list =[]
        for file in files:
            self.dependencies.append(self.wq.get_worker())
            text_list.append(TextFile(file))
        for i in text_list:
            i.set_dependencies(self.dependencies)
        return text_list

    def map(self,parent,func):
        map_list = []
        for i in parent:
            map_list.append(Map(i,func))
        return map_list

    def flatmap(self,parent,func):
        flatmap_list = []
        for i in parent:
            flatmap_list.append(FlatMap(i,func))
        return flatmap_list

    def groupbykey(self,parent):
        groupbykey_list = []
        for i in parent:
            groupbykey_list.append(GroupByKey(i))
        return groupbykey_list

    def sort(self,parent):
        sort_list=[]
        for i in parent:
            sort_list.append(Sort(i))
        return sort_list

    def filter(self,parent,func):
        filter_list = []
        for i in parent:
            filter_list.append(Filter(i,func))
        return filter_list


    def join(self,p1,parent1,parent2,first_only = False):
        join_list = []
        #print len(parent2)
        if not first_only:
            self.dependencies = sorted(set(self.dependencies + p1.dependencies))
            p1.dependencies = self.dependencies
        else:
            self.dependencies = sorted(set(self.dependencies + p1.dependencies))
        for i in parent2:
            join_list.append(Join(i,parent1[0] )) # Another very tricky part, I will explain
        for i in parent1:
            join_list.append(Join(i,parent2[0]))# very very tricky part :D
        for i in join_list:
            i.cur_depend = self.dependencies
        return join_list


    def execute(self,parent):
        for i_index, i  in enumerate(parent):
            i_obj = self.serialize(i)
            Parallel.para_worker_dict[self.dependencies[i_index]] = i_obj
            if self.fail_test:
                self.gevent_list.append(gevent.spawn(self.wq.start_job_fail_test,self.dependencies[i_index],i_obj))
            else:
                self.gevent_list.append(gevent.spawn(self.wq.start_job,self.dependencies[i_index],i_obj))
        #self.wq.start_server()
        self.wq.m.gevent_list = self.gevent_list
        gevent.joinall(self.gevent_list)
        count = 1
        while count != 0 :
            count = 0
            count += sum([0 if i.value != None else 1 for i in self.gevent_list])
        return [i.value if i.value != None else []for i in self.gevent_list]

    def serialize(self,obj):
        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(obj)
        return output.getvalue()



def join_sort_test(ec2=False):
    if (ec2):
        wq=EC2Worker.EC2Worker()
    else:
        wq = WorkerQueue()
    p = Parallel(wq)
    s = p.textFile('./Data')
    s = p.map(s,lambda x : x.split())
    s = p.flatmap(s, lambda x : [x , '1'])
    p1 = Parallel(wq)
    s1 = p1.textFile('./Data1')
    s1 = p1.map(s1,lambda x : x.split())
    s1 = p1.flatmap(s1, lambda x : [x , '1'])
    s = p.join(p1,s1,s)
    p2 = Parallel(wq)
    s2 = p2.textFile('./Data2')
    s2 = p2.map(s2,lambda x : x.split())
    s2 = p2.flatmap(s2, lambda x : [x , '1'])
    #s = p.groupbykey(s)
    #s = p.join(p2,s2,s)
    s = p.map(s, lambda x : [x[0] , sum(map(int,x[1]))])
    s = p.sort(s)
    s = p.execute(s)
    for i in s :
         for j in i :
             print j[0], j[1]



def height_test():
    wq = WorkerQueue()
    p = Parallel(wq)
    s = p.textFile('./Data')
    s = p.map(s,lambda x : x.split())
    s = p.flatmap(s, lambda x : [x , '1'])
    p1 = Parallel(wq)
    s1 = p1.textFile('./Data1')
    s1 = p1.map(s1,lambda x : x.split())
    s1 = p1.flatmap(s1, lambda x : [x , '1'])
    s = p.join(p1,s1,s)
    p2 = Parallel(wq)
    s2 = p2.textFile('./Data2')
    s2 = p2.map(s2,lambda x : x.split())
    s2 = p2.flatmap(s2, lambda x : [x , '1'])
    #s = p.groupbykey(s)
    s = p.join(p2,s2,s)
    #s = p.groupbykey(s)
    s = p.map(s, lambda x : [x[0] , sum(map(int,x[1]))])
    s = p.sort(s)
    s = p.execute(s)
    for i in s :
         for j in i :
             print j[0], j[1]


def failure_test(no_fail=False,ec2=False):
    if (ec2):
        wq=EC2Worker.EC2Worker()
    else:
        wq = WorkerQueue()
    p = Parallel(wq)
    if not no_fail:
        p.fail_test = True
    s = p.textFile('./Data')
    s = p.map(s,lambda x : x.split())
    s = p.flatmap(s, lambda x : [x , '1'])
    s = p.groupbykey(s)
    s = p.map(s, lambda x : [x[0] , sum(map(int,x[1]))])
    s = p.filter(s,lambda x :  x[1] > 1000)
    #s = p.sort(s)
    s = p.execute(s)
    for i in s :
         for j in i :
             print j[0], j[1]



def zero_rpc_exception_throw_test():
    wq = WorkerQueue()
    system('python zero_rpc_rais_test.py &')


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = urls.split()
    return [parts[0], parts[1]]

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    temp = []
    for url in urls:
        temp.append( [url, rank / num_urls])
    return temp

def url_rank_test():
    wq = WorkerQueue()
    p = Parallel(wq)
    lines = p.textFile('./urls')
    links = p.map(lines,lambda urls: parseNeighbors(urls))
    links = p.groupbykey(links)
    p1 = Parallel(wq)
    lines1 = p1.textFile('./urls')
    links1 = p1.map(lines1,lambda urls: [parseNeighbors(urls)[0],1])
    links1 = p1.groupbykey(links1)
    links1 = p1.map(links1,lambda urls: [urls[0],sum(map(int, urls[1]))] )
    links1 = p1.join(p,links1,links,first_only=True)
    links1 = p1.map(links1,lambda x : [x[0], x[1] if type(x[1][0]) == int else [i for i in reversed(x[1])]])
    links1 = p1.map(links1, lambda url_urls_rank: [url_urls_rank[0],[computeContribs(url_urls_rank[1][1:],
                                                                                  int(url_urls_rank[1][0])),
                                                                     url_urls_rank[1][1:]]])
    links1 = p1.map(links1,lambda x : [x[0],int(sum(map(lambda x : x[1],(x[1][0])))),x[1][1]])
    links1 = p1.map(links1, lambda x : [x[0],x[1]*0.85+0.15,x[2]])
    for i in range(10):
         links1 = p1.map(links1, lambda url_urls_rank: [url_urls_rank[0],[computeContribs(url_urls_rank[2][:],
                                                                                     url_urls_rank[1]),
                                                                       url_urls_rank[2][:]]])
         links1 = p1.map(links1,lambda x : [x[0],(sum(map(lambda x : x[1],(x[1][0])))),x[1][1]])
         links1 = p1.map(links1, lambda x : [x[0],x[1]*0.85+0.15,x[2]])
    links1 = p1.map(links1 , lambda x : [x[1],x[0]])
    links1 = p1.sort(links1)
    links1 = p1.map(links1 , lambda x : [x[1]])
    links1 = p1.execute(links1)
    for i in links1 :
         for j in i:
             print j[0]




if __name__ == '__main__':

    parse=argparse.ArgumentParser()
    parse.add_argument("--fail", action="store_true")
    parse.add_argument("--nofail", action="store_true")
    parse.add_argument("--pagerank",action="store_true")
    parse.add_argument("--htest",action="store_true")

    # Run on EC2 Node
    parse.add_argument("--ec2", action="store_true")
    parse.add_argument("--master")


    args=parse.parse_args()
    if (args.ec2 and not(args.master)):
        print "Must specify master ip address if ec2 mode is active"
        exit()
    if (args.master):
        RDD.master=args.master

    #zero_rpc_exception_throw_test()
    if args.fail:
        failure_test(ec2=args.ec2)
        WorkerQueue.g.join()
    elif args.nofail:
        failure_test(ec2=args.ec2,no_fail=True)
        WorkerQueue.g.join()
    elif args.pagerank:
        url_rank_test()
    elif args.htest:
        height_test()
    else:
        join_sort_test(ec2=args.ec2)





    #print s








#!/usr/bin/python
import pickle
import StringIO

import zerorpc
import cloudpickle

from rdd import *
from os import listdir
from os.path import isfile, join
from gevent import Greenlet

def start_job(count,ob):
    c = zerorpc.Client()
    c.connect("tcp://127.0.0.1:"+str(count))
    ttt = c.hello(ob)
    return ttt


class Parallel(object):

    port = 4242

    def __init__(self):
        self.dependencies=[]

    def textFile(self,mypath):
        files = [ join(mypath,f) for f in listdir(mypath) if isfile(join(mypath,f))]
        text_list =[]
        for file in files:
            self.dependencies.append(Parallel.port)
            Parallel.port +=1
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


    def join(self,parent1,parent2):
        join_list = []
        for i in parent1:
            join_list.append(Join(i,parent2[0]))
        for i in parent2:
            join_list.append(Join(i,parent1[0]))
        return join_list


    def execute(self,parent):
        g_list = []
        p = 4242
        count = 0
        for i in parent:
            i_obj = self.serialize(i)
            g_list.append(gevent.spawn(start_job,count+p,i_obj))
            count+=1
        gevent.joinall(g_list)
        return [i.value for i in g_list]

    def serialize(self,obj):
        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(obj)
        return output.getvalue()




if __name__ == '__main__':

    p = Parallel()
    s = p.textFile('./Data')
    s = p.map(s,lambda x : x.split())
    s = p.flatmap(s, lambda x : [x , '1'])
    s = p.groupbykey(s)
    s = p.map(s, lambda x : [x[0] , sum(map(int,x[1]))])
    s = p .filter(s, lambda x : x[1] > 10)
    s = p.sort(s)
    # p1 = Parallel()
    # s1 = p1.textFile('./Data1')
    # s1 = p1.map(s1,lambda x : x.split())
    # #s1 = p1.flatmap(s1, lambda x : [x , '1'])
    # #s1 = p1.groupbykey(s1)
    # #s1 = p1.map(s1, lambda x : [x[0] , sum(map(int,x[1]))])
    # #s = p .filter(s, lambda x : x[1] > 10)
    # #s = p.sort(s)
    # s = p.join(s1,s)
    s = p.execute(s)
    for i in s :
        for j in i :
            print j[0],j[1]


    #print s








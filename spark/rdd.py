import zerorpc
import gevent
import numpy.random as rand
import StringIO
import cloudpickle

#!/usr/bin/python
class RDD(object):

    wd = []

    def __init__(self):
        self.id = None
        self.id_range = None



    def set_id(self,port):
        self.parent.set_id(port)

    def set_cur_id(self,port):
        self.id = int(port)

    def set_id_range(self,start,end):
        self.parent.set_id_range(start,end)

    def set_cur_id_range(self, start, end):
        self.id_range = range(start,end)

    def get_partitions(self):
        return self.parent.get_partitions()

    def wide_dependency_get_partitions(self, parent_list):

        parent_dependencies = []
        for parent in parent_list:
            parent_dependencies += parent.get_partitions()

        return parent_dependencies

    def narrow_dependency_get_partitions(self):
        return self.parent.get_partitions()

    def partitions(self):
        pass

    def get_connection(self,port):
        self.c.connect("tcp://127.0.0.1:"+str(port))


    def prefferedLocations(self,p):
        pass

    def get_dependencies(self): # We need to fix this part so when we have a wide dependency we know exactly which
        #machines to ask for data
        return self.dependencies
        pass

    def iterator(self):
        pass

    def partitioner(self):
        pass

    def get_id_range(self):
        return self.parent.get_id_range()

    def get_cur_id_range(self):
        return self.id_range

    def get_id(self):
        return self.parent.get_id()

    def cur_id(self):
        return self.id

    def set_persist(self):
        self.persist = True

    def collect(self):
        elements = []
        for elem in self.iterator():
            elements.append(elem)
        return elements

    def wide_iter(self):
        while self.status != 'Done':
            gevent.sleep(0.0001)
            yield None
        for i in self.data:
            yield i


    #def collect(self):
        #gevent.spawn(self.g_collect())


    def serialize(self,obj):
        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(obj)
        return output.getvalue()

    def fetch_data(self , turn = 0,hashfunc = lambda a : hash(a)%7 + 4242, fetch_all = False,partitions = None,
                   join = False, forced = False):
        if self.data_wide == None:
            self.data_wide = 'setting'
            if self.c == None:
                self.c = zerorpc.Client()

            temp_partitions = []
            if not partitions:
                for i in self.get_partitions():
                    if i != self.get_id():
                        temp_partitions.append([i,False])
            else:
                for i in partitions:
                    temp_partitions.append([i,False])

            fetched_data = []
            geven_lis = []
            for i_index , i in enumerate(temp_partitions):
                self.get_connection(i[0])
                ser_hash = self.serialize(hashfunc)
                if not join :
                    geven_lis.append(gevent.spawn(self.c.get_data,[self.get_id(),turn],self.height,ser_hash,
                                                        fetch_all,forced))
                else:
                    res = self.c.get_data([self.get_id(),turn],join,ser_hash,fetch_all,forced)
            gevent.joinall(geven_lis)
            for i in geven_lis:
                for j in i.value:
                    fetched_data.append(j)
            for i in self.data :
                if not fetch_all:
                    if i :
                        if hashfunc(i[0]) == self.get_id():
                            fetched_data.append(i)
                else:
                    if i :
                        fetched_data.append(i)
            self.data_wide = fetched_data
            #fetched_data=[]


    def calculate_narrow(self):
        if self.data == None:
            temp_data = []
            for elem in self.parent.iterator():
                temp_data.append(elem)

            self.data = temp_data
            self.status = 'Done'







    def get_data(self,height):
        #print height
        #print "self", self.height
        if type(height) == int :
            if self.wide and height == self.height:
                return self.wide_iter()
            else:
                return self.parent.get_data(height)
        else:
            return self.iterator()

    def count(self):
        return len(self.collect())



class Sample(RDD):

    def __init__(self,parent , size = 10):
        self.parent = parent
        self.data = None
        self.wide = True
        self.size = size
        self.sample_data = None
        self.height = 0
        self.status = None
        self.fetched_count = 0
        self.data_wide = None
        self.c = None

    def iterator(self):
        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        self.calculate_narrow()
        if len(self.data) > 0 and self.status =='Done':
            if self.size < len(self.data) :
                indexes = rand.choice(range(len(self.data)),self.size
                ,replace = False)
                self.data = [self.data[i] for i in indexes]

        #print "height here" , self.height
        self.fetch_data(fetch_all=True)
       # print self.data_wide
        for i in self.data_wide:
            yield  i

class Join(RDD):

    def __init__(self,parent,other):
        self.parent = parent
        self.other = other
        self.data = None
        self.c = None
        self.wide = True
        self.data_wide= None
        self.height = 0
        self.status = None
        self.fetched_count = 0

    def iterator(self):
        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        if self.data_wide == None:
            self.calculate_narrow()
            self.fetch_data(join = True,fetch_all= True , partitions=[self.other.get_id()])
        for i in self.data_wide:
            yield i


class Sort(RDD):

    def __init__(self,parent,reverse = False):# we have to use parent for dependencies which we are not !!
        self.parent = parent
        self.data = None
        self.c = None
        self.wide = True
        self.data_wide = None
        self.height = 0
        self.reverse = reverse
        self.status = None
        self.fetched_count = 0

    def iterator(self):

        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        if self.data_wide == None :
            s_sample = Sample(self.parent,size = 3)
            temp_parent = self.parent
            self.parent = s_sample
            sample_data= []
            for i in s_sample.iterator():
                sample_data.append(i)
            self.parent = temp_parent
            self.calculate_narrow()
            temp_sorted = sorted(sample_data)

            def hash_func(x):
                # We are explicitly using value 6 which we have to fix after we fix dependencies
                tmp = 0
                count = 0
                for i in temp_sorted:
                    if x > i[0]:
                        tmp = count
                        count+=1
                        continue
                    break
                bucket_length = len(temp_sorted)/7.
                return int(tmp /bucket_length) + 4242

            self.fetch_data(turn = 1,hashfunc= hash_func)
            self.data_wide = sorted(self.data_wide, reverse = self.reverse)
        for i in self.data_wide:
            yield i

            #self.fetch_data(fetch_all=True)






class GroupByKey(RDD):

    def __init__(self,parent):
        self.parent = parent
        self.data = None
        self.c = None
        self.wide = True
        self.data_wide = None
        self.height = 0
        self.status = None
        self.fetched_count = 0




    def iterator(self):
        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        if self.data_wide == None:
            self.calculate_narrow()
            self.fetch_data()
            temp_dict = {}
            for i in self.data_wide :
                if i[0] not in temp_dict:
                    temp_dict[i[0]] = []
                temp_dict[i[0]].extend(i[1])
            temp_data_wide = []
            for k,v in temp_dict.iteritems():
                temp_data_wide.append([k,v])
            self.data_wide = list(temp_data_wide)
        while self.data_wide == 'setting':
            gevent.sleep(0.001)
            #print "sss"
            yield []
        for i in self.data_wide :
            if len (i) > 0  :
                yield i








class TextFile(RDD):

    def __init__(self, filename):
        self.filename = filename
        self.data = None
        self.index = 0
        self.wide = False
        self.height = 0
        self.status = None
        self.fetched_count = 0

    def get_id(self):
        return super(TextFile,self).cur_id()

    def get_id_range(self):
        return super(TextFile,self).get_cur_id_range()

    def set_id(self,port):
        super(TextFile,self).set_cur_id(port)

    def set_id_range(self,start,end):
        super(TextFile,self).set_cur_id_range(start,end)

    def get_partitions(self):
        return super(TextFile,self).get_cur_id_range()

    def get_data(self):
        # get data for text file
        pass

    def get(self):
        if not self.data:
            f = open(self.filename)
            self.data = f.readlines()
            f.close()
    
    def iterator(self):
        RDD.wd.append(self.wide)
        self.height = len(RDD.wd)
        self.get()
        for line in self.data:
            yield line

class FlatMap(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.data = []
        self.persist = False
        self.wide = False
        self.height = 0
        self.status = None
        self.fetched_count = 0



    def iterator(self):
        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        if (len(self.data) == 0 or not self.persist):
            for elem in self.parent.iterator():
                if type(elem) == list:
                    for _ in elem :
                        if self.persist:
                            self.data.append(self.func(_) )
                        yield self.func(_)
                else:

                    _ = self.func(elem)

                    if self.persist:
                        self.data.append(_)
                    yield _
        else:
            for _ in self.data:
                yield _


class Union(RDD):

    def __init__(self, parent1,parent2):
        self.parent1 = parent1
        self.parent2 = parent2
        self.data = []
        self.persist = False
        self.wide = False
        self.height = 0
        self.status = None
        self.fetched_count = 0

    def iterator(self):
        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        if (len(self.data) == 0 or not self.persist):
            for elem in self.parent1.iterator():
                if self.persist:
                    self.data.append(elem)
                yield elem
            for elem in self.parent2.iterator():
                if self.persist:
                    self.data.append(elem)
                yield elem
        else:
            for _ in self.data:
                yield _




class Map(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.data = []
        self.persist = False
        self.wide = False
        self.height = 0
        self.status = None
        self.fetched_count = 0


    def iterator(self):
        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        if (len(self.data) == 0 or not self.persist):
            for elem in self.parent.iterator():
                _ = self.func(elem)
                if self.persist:
                    self.data.append(_)
                yield _
        else:
            for _ in self.data:
                yield _

class Filter(RDD):
    
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.persist = False
        self.wide = False
        self.data = []
        self.height = 0
        self.status = None
        self.fetched_count = 0

    def iterator(self):
        RDD.wd.append(self.wide)
        if self.height == 0 :
            self.height = len(RDD.wd)
        if (len(self.data) == 0 or not self.persist):
            for _ in self.parent.iterator():
                if self.func(_) :
                    if self.persist:
                        self.data.append(_)
                    yield _
        else:
            for _ in self.data:
                yield _



if __name__ == '__main__':

    r = TextFile('./Data/myfile')
    m = Map(r, lambda s: s.split())
    m.set_persist()
    f = Filter(m, lambda a: int(a[1]) > 2)
    print f.collect()

    f = Filter(m, lambda a: int(a[1]) > 2)
    print f.count()










import zerorpc
import gevent
import numpy.random as rand
import StringIO
import cloudpickle

#In case we want to plot we just change these
Graph = True
Time = 0.001
Timeout = 10

#Hack for dealing with ip+port when remote and port when local
def get_port(s):
    if (str(s).startswith("tcp")):
        return int(s.split(":")[2])
    return s

#!/usr/bin/python
class RDD(object):

    master= "127.0.0.1"
    wd = []
    port_to_url={}

    def connect_master(self,value):
        c = zerorpc.Client()
        c.connect("tcp://"+RDD.master+":4241")
        c.add_failed_nodes(value)
        c.close()

    def connect_plotter(self,s,t,height_s,height_t): # source, target , height
        c = zerorpc.Client()
        c.connect("tcp://0.0.0.0:4240")
        c.plot_graph(s,t,height_s,height_t,self.name)
        c.close()

    def __init__(self):
        self.id = None
        self.id_range = None
        self.depend = None


    def merge_dependencies(self,p1,p2):
        depend1 = p1.get_dependencies()[:]
        depend2 = p2.get_dependencies()[:]
        res = depend1 + depend2
        res = sorted(res)
        #print "self id",self.get_id(),"dependencies",res
        return res

    def set_id(self,port):
        self.parent.set_id(port)

    def set_cur_id(self,port):
        self.id = int(port)

    def set_id_range(self,start,end):
        self.parent.set_id_range(start,end)

    def set_dependencies(self,depend_list):
        self.parent.set_dependencies(depend_list)

    def set_cur_dependencies(self,depend_list):
        self.depend = depend_list

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
        if len(RDD.port_to_url)>0:
            self.c.connect(RDD.port_to_url[port])
        else:
            self.c.connect("tcp://127.0.0.1:"+str(port))


    def prefferedLocations(self,p):
        pass


    def iterator(self):
        pass

    def partitioner(self):
        pass

    def get_dependencies(self):
        if self.cur_depend == None:
            return self.parent.get_dependencies()
        #print "return self cur depent" , self.cur_depend
        return self.cur_depend

    def get_cur_dependencies(self):

        return self.depend

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
        RDD.wd=[]
        elements = []
        self.height = 0
        for elem in self.iterator():
            elements.append(elem)
        return elements

    def wide_iter(self):
        while self.data == None:
            gevent.sleep(Time)
            yield None
        for i in self.data:
            yield i
        self.wide_count+=1

    def serialize(self,obj):
        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(obj)
        return output.getvalue()


    def fetch_data(self , turn = 0,hashfunc = None, fetch_all = False,partitions = None,
                   join = False, forced = False):

        if hashfunc == None:
            first = self.get_dependencies()[0]
            depend_len = len(self.get_dependencies())
            def default_hash(x):
                return hash(x)% depend_len + get_port(first)
            hashfunc = default_hash
        ser_hash = self.serialize(hashfunc)
        potential_fail = []
        if self.data_wide == None:
            self.data_wide = 'setting'
            if self.c == None:
                self.c = zerorpc.Client(timeout=Timeout)

            fetched_data = []
            geven_lis = []
            for i in self.get_dependencies():
                if i == self.get_id():
                    continue
                self.get_connection(i)
                if Graph:
                    self.connect_plotter(self.get_id(),i,self.height,self.height)
                geven_lis.append(gevent.spawn(self.c.get_data,[self.get_id(),turn],self.height,ser_hash,
                                                  fetch_all,forced))
                potential_fail.append(i)
            for i in self.data :
                if not fetch_all:
                    if i :
                        if hashfunc(i[0]) == self.get_id():
                            fetched_data.append(i)
                else:
                    if i :
                        fetched_data.append(i)
            gevent.joinall(geven_lis) # Unfortunatly joinall does not RAISE the exception. It uses traceback.print
            # to print the exception without and file defined so it will print it to sys.err so no way for us to get
            # around the terminal exceptions throwing FOR NOW
            failed_list = []
            for i_index,i in enumerate(geven_lis):
                if i.value== None:
                    failed_list.append(potential_fail[i_index])
                    continue
                fetched_data.extend(i.value)
            while len(failed_list) != 0 :
                self.connect_master([i for i in failed_list])
                gevent.sleep(Time)
                geven_lis = []
                potential_fail=[]
                for i in failed_list:
                    self.get_connection(i)
                    geven_lis.append(gevent.spawn(self.c.get_data,[self.get_id(),turn],self.height,ser_hash,
                                                        fetch_all,forced))
                    potential_fail.append(i)
                gevent.joinall(geven_lis)
                failed_list = []
                for i_index,i in enumerate(geven_lis):
                    if i.value == None:
                        failed_list.append(potential_fail[i_index])
                        continue
                    fetched_data.extend(i.value)
            self.data_wide = fetched_data

    def calculate_narrow(self):
        if self.data == None:
            temp_data = []
            for elem in self.parent.iterator():
                temp_data.append(elem)
            self.data = temp_data
            self.status = 'Done'

    def get_data(self,height):
        #print "wide,self ,s_height, height,self.parent",self,self.wide,self.height,height,self.parent
        if self.wide and height == self.height:
            #print "Im in   =--------"
            return self.wide_iter()
        else:
            return self.parent.get_data(height)

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
        self.cur_depend = None
        self.wide_count = 0
        self.name = 'Sample'

    def iterator(self):
        if self.parent.height != 0 :
            self.parent.height = self.height +1
        self.calculate_narrow()
        if len(self.data) > 0 and self.status =='Done':
            if self.size < len(self.data) :
                indexes = rand.choice(range(len(self.data)),self.size
                ,replace = False)
                self.data = [self.data[i] for i in indexes]

        self.fetch_data(fetch_all=True)
        for i in self.data_wide:
            yield  i
        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)

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
        self.cur_depend = None
        self.wide_count = 0
        self.name = 'Join'

    def iterator(self):
        self.parent.height = self.height +1
        if self.data_wide == None:
            self.calculate_narrow()
            self.fetch_data()
            temp_dict = {}
            for i in self.data_wide :
                if i[0] not in temp_dict:
                    temp_dict[i[0]] = []
                if type(i[1]) != list:
                    i[1] = [i[1]]
                temp_dict[i[0]].extend(i[1])
            temp_data_wide = []
            for k,v in temp_dict.iteritems():
                temp_data_wide.append([k,v])
            self.data_wide = list(temp_data_wide)
        for i in self.data_wide:
            yield i
        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)


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
        self.cur_depend = None
        self.wide_count = 0
        self.name = 'Sort'

    def iterator(self):

        self.parent.height = self.height +1
        if self.data_wide == None :
            s_sample = Sample(self.parent,size = 5)
            temp_parent = self.parent
            self.parent = s_sample
            self.parent.height = self.height +1
            sample_data= []
            for i in s_sample.iterator():
                sample_data.append(i)
            while(self.parent.wide_count != (len(self.get_dependencies())-1)):
                gevent.sleep(Time)
            # I am not sure if we need this while loop. DO NOT DELETE IT YET
            self.parent = temp_parent
            self.calculate_narrow()
            temp_sorted = sorted(sample_data)
            depend_len = len(self.get_dependencies())
            first = self.get_dependencies()[0]
            def hash_func(x):
                # We are explicitly using value 7 which we have to fix after we fix dependencies
                tmp = 0
                count = 0
                for i in temp_sorted:
                    if x > i[0]:
                        tmp = count
                        count+=1
                        continue
                    break
                bucket_length = len(temp_sorted)/depend_len
                return (tmp /bucket_length) + first
            self.fetch_data(hashfunc= hash_func)
            self.data_wide = sorted(self.data_wide, reverse = self.reverse)
        for i in self.data_wide:
            yield i
        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)







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
        self.cur_depend = None
        self.wide_count = 0
        self.name = 'GroupByKey'




    def iterator(self):
        self.parent.height = self.height +1
        if self.data_wide == None:
            self.calculate_narrow()
            #print self.get_id()
            #print self.data
            self.fetch_data()
            temp_dict = {}
            for i in self.data_wide :
                if i[0] not in temp_dict:
                    temp_dict[i[0]] = []
                if type(i[1]) != list:
                    i[1] = [i[1]]
                temp_dict[i[0]].extend(i[1])
            temp_data_wide = []
            for k,v in temp_dict.iteritems():
                temp_data_wide.append([k,v])
            self.data_wide = list(temp_data_wide)
        while self.data_wide == 'setting':
            gevent.sleep(Time)
            yield []
        for i in self.data_wide :
            if len (i) > 0  :
                yield i
        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)








class TextFile(RDD):

    def __init__(self, filename):
        self.filename = filename
        self.data = None
        self.index = 0
        self.wide = False
        self.height = 0
        self.status = None
        self.fetched_count = 0
        self.cur_depend = None
        self.name = 'TextFile'

    def set_dependencies(self,depend_list):
        super(TextFile,self).set_cur_dependencies(depend_list)

    def get_dependencies(self):
        return super(TextFile,self).get_cur_dependencies()

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
        if self.height == 0 :
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
        self.cur_depend = None
        self.name = 'FlatMap'



    def iterator(self):
        self.parent.height = self.height +1
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
        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)


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
        self.cur_depend = None
        self.name = 'Union'

    def iterator(self):
        self.parent.height = self.height +1
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
        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)




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
        self.cur_depend = None
        self.name = 'Map'


    def iterator(self):
        self.parent.height = self.height +1

        if (len(self.data) == 0 or not self.persist):
            for elem in self.parent.iterator():
                _ = self.func(elem)
                if self.persist:
                    self.data.append(_)
                yield _
        else:
            for _ in self.data:
                yield _
        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)

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
        self.cur_depend = None
        self.name = 'Filter'

    def iterator(self):
        self.parent.height = self.height +1
        if (len(self.data) == 0 or not self.persist):
            for _ in self.parent.iterator():
                if self.func(_) :
                    if self.persist:
                        self.data.append(_)
                    yield _
        else:
            for _ in self.data:
                yield _

        if Graph:
                self.connect_plotter(self.get_id(),self.get_id(),self.height,self.parent.height)



if __name__ == '__main__':

    r = TextFile('./Data/myfile')
    m = Map(r, lambda s: s.split())
    m.set_persist()
    f = Filter(m, lambda a: int(a[1]) > 2)
    print f.collect()

    f = Filter(m, lambda a: int(a[1]) > 2)
    print f.count()










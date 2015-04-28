import zerorpc
import gevent
#!/usr/bin/python
class RDD(object):

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

    def dependencies(self):
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


    #def collect(self):
        #gevent.spawn(self.g_collect())


    def get_data(self):
        if self.data :
            res = self.data
            #self.data = {}
            return res
        else:
            return None

    def count(self):
        return len(self.collect())


class GroupByKey(RDD):

    def __init__(self,parent):
        self.parent = parent
        self.data = {}
        self.c = None
        pass

    def iterator(self):

        for elem in self.parent.iterator():
            if elem[0] not in self.data :
                self.data[elem[0]] = [elem[1]]
            else:
                self.data[elem[0]].append(elem[1])
            #yield elem

        for i in self.fetch_data():
            yield i



    def fetch_data(self):
        shared_data = {}
        if self.c == None:
            self.c = zerorpc.Client()
        temp_partitions = []
        for i in self.get_partitions():
            if i != self.get_id():
                temp_partitions.append([i,False])
        fetched_data = []
        while True:
            gevent.sleep(0.01)

            for i_index , i in enumerate(temp_partitions):
                if i[1] == False:
                    self.get_connection(i[0])
                    res = self.c.get_data(self.get_id())
                    if res != None  :
                        temp_partitions[i_index][1] = True
                        if res != 'No Partition':
                            fetched_data.extend(res)
            count = 0
            for i in temp_partitions:
                if i[1] == True:
                    count+=1
            if count == len(temp_partitions) :
                break
        for i in  fetched_data:
            yield i
        for i in self.data :
            if hash(i)%5 + 4242 == self.get_id():
                yield [i , self.data[i]]



class TextFile(RDD):

    def __init__(self, filename):
        self.filename = filename
        self.data = None
        self.index = 0

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

    def get(self):
        if not self.data:
            f = open(self.filename)
            self.data = f.readlines()
            f.close()
    
    def iterator(self):
        self.get()
        for line in self.data:
            yield line

class FlatMap(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.data = []
        self.persist = False



    def iterator(self):
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

    def iterator(self):
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


    def iterator(self):
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
        self.data = []

    def iterator(self):
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










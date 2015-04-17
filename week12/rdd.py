
class RDD(object):

    def __init__(self):
        pass

    def getPartition(self):
        pass

    def getIterator(self,partition):
        pass

    def getPreferredLocations(self,partition):
        pass

    def getDependencies(self):
        pass

    def getPartitioner(self):
        pass

    def collect(self):
        elements = []
        for i in self.getIterator(1):
            elements.append(i)
        return elements

    def count(self):
        return len(self.collect())

class TextFile(RDD):

    def __init__(self, filename):
        #This is a single file implementation only
        self.filename = filename
        self.lines = []
        self.index = 0


    def getPartition(self):
        f = open(self.filename,"r")
        self.lines = f.readlines()
        f.close()
        return self.filename

    def getIterator(self,partition):
        self.getPartition()
        for i in self.lines:
            yield i


class Map(RDD):


    def __init__(self, parent, func):
        self.parent = parent
        self.func = func


    def getIterator(self,partition):
        for i in self.parent.getIterator(partition):
            yield self.func(i)

class Filter(RDD):
    
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def getIterator(self,partition):
        for i in self.parent.getIterator(partition):
            if self.func(i):
                yield i


if __name__ == '__main__':

    r = TextFile('myfile')
    m = Map(r, lambda s: s.split())
    f = Filter(m, lambda a: int(a[1]) > 2)
    print r.collect()








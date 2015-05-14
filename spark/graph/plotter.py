
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors
import zerorpc
import copy
import sys

g_list = []
color_dict = {'Sample':[i/255.for i in [213,62,79]],'TextFile' : [i/255.for i in [244,109,67]],
              'GroupByKey':[i/255.for i in [253,174,97]],'Map':[i/255.for i in [254,224,139]]
              ,'FlatMap':[i/255.for i in [230,245,152]],'Filter':[i/255.for i in [171,221,164]]
    ,'Join':[i/255.for i in [102,194,165]],'Sort':[i/255.for i in [50,136,189]]}

import matplotlib.patches as mpatches

class Graph:

    def __init__(self,steps,steps_shown):
        self.nodes = set()
        self.G = nx.Graph()
        self.pos={}
        self.min_ip=4242
        self.lables = {}
        self.edge_names = {}
        self.colors =[]
        plt.ion()
        self.k = 0
        self.step = int(steps)
        self.steps_shown = steps_shown




    def plot_graph(self,s,t,height_s,height_t,name_s,name_t,saved    ):

        if height_t != -1:

            self.G.add_node(str(s)+str(height_s),color = color_dict[name_s],size = saved)

            if (str(t) + str(height_t)) not in self.nodes:
                self.G.add_node(str(t)+str(height_t),color = color_dict[name_t], size = 0)

            self.nodes.add(str(s) + str(height_s))
            self.nodes.add(str(t) + str(height_t))

            if s not in self.pos:
                self.pos[str(s)+str(height_s)] = [(int(s)-self.min_ip)*10,height_s*2]

            if t not in self.pos:
                self.pos[str(t)+str(height_t)] = [(int(t)-self.min_ip)*10,height_t*2]


            if s not in self.lables:
                self.lables[str(s)+str(height_s)] = str(s)

            if t not in self.lables:
                self.lables[str(t)+str(height_t)] = str(t)


            self.G.add_edge(str(s)+str(height_s),str(t)+str(height_t))
            self.colors.append(color_dict[name_s])

            self.edge_names[(str(s)+str(height_s),str(t)+str(height_t))] = 'hello'

        else :
            self.G.add_node('MASTER', color = 'w')
            self.pos['MASTER'] = [-1,5]
            self.G.add_edge(str(s)+str(height_s),'MASTER')


        #nx.draw_networkx(self.G, pos = self.pos)
        if self.steps_shown:
            self.k +=1
            if self.k % self.step == 0:
                self.add()
            # show graph

    def add(self):
        g_list.append(copy.deepcopy(self))


    def done(self):
        for l in g_list if self.steps_shown else [self]:
            nodes = l.G.nodes(data= True)
            color_list = []
            node_shapes = []
            size = []
            import math
            for i in nodes :
                color_list.append(i[1]['color'])
                node_shapes.append("r")
                if 'size' in i[1]:
                    size.append(math.log(i[1]['size']+1)*100 + 100)
            nx.draw_networkx(l.G, pos = l.pos,labels =l.lables,edge_lables=l.edge_names,node_size = size,
                             font_size = 8,node_color = color_list)
            #red_patch = mpatches.Patch(color='r', label='The red data')
            legends = []
            for i in color_dict:
                legends.append(mpatches.Patch(color =color_dict[i],label = i))
            plt.legend(handles=legends,fontsize = 'small')

            if self.steps_shown:
                plt.figure(num=0,figsize=(16,8))
                plt.draw()

        plt.ioff()
        plt.show()



s = zerorpc.Server(Graph(sys.argv[1],True if len(sys.argv)>2 else False))
s.bind("tcp://0.0.0.0:4240")
s.run()


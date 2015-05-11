import networkx as nx
import matplotlib.pyplot as plt
import zerorpc

color_dict = {'Sample':'b','Union' : 'g','GroupByKey':'r','Map':'c','FlatMap':'m','Filter':'y','Join':'k','Sort':'w','TextFile':'g'}


class Graph:

    def __init__(self):
        self.nodes = set()
        self.G = nx.Graph()
        self.pos={}
        self.min_ip=4242
        self.lables = {}
        self.edge_names = {}
        self.colors =[]



    def plot_graph(self,s,t,height_s,height_t,name):

        self.G.add_node(str(s)+str(height_s),color = color_dict[name])

        self.G.add_node(str(t)+str(height_t),color = color_dict[name])

        if s not in self.pos:
            self.pos[str(s)+str(height_s)] = [(int(s)-self.min_ip)*10,int(height_s)*2]

        if t not in self.pos:
            self.pos[str(t)+str(height_t)] = [(int(t)-self.min_ip)*10,int(height_t)*2]


        if s not in self.lables:
            self.lables[str(s)+str(height_s)] = str(s)

        if t not in self.lables:
            self.lables[str(t)+str(height_t)] = str(t)


        self.G.add_edge(str(s)+str(height_s),str(t)+str(height_t))
        self.colors.append(color_dict[name])

        self.edge_names[(str(s)+str(height_s),str(t)+str(height_t))] = 'hello'


        #nx.draw_networkx(self.G, pos = self.pos)

        # show graph


    def done(self):
        nodes = self.G.nodes(data= True)
        color_list = []
        for i in nodes :
            color_list.append(i[1]['color'])
        nx.draw_networkx(self.G, pos = self.pos,labels =self.lables,edge_lables=self.edge_names,node_size = 800,
                         font_size = 8,node_color = color_list)
        plt.legend([1,2,3,4], ["Master","Worker","Mapper","Reducer"],loc = 2)
        plt.show()


s = zerorpc.Server(Graph())
s.bind("tcp://0.0.0.0:4240")
s.run()

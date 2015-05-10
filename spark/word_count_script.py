from os import listdir
from os.path import isfile, join
import sys
mypath = []
for i in sys.argv[1:]:
	mypath.append(i)
files = []
for i in mypath:
	files.extend(join(i,f) for f in listdir(i) if isfile(join(i,f)))
count_dict = {}	
for file in files:
	f = open(file, 'r')
	f_read = f.readlines()
	f.close()
	for i in f_read:
		k = i.split()
		for i in k : 
			if i not in count_dict:
				count_dict[i] = 0
			count_dict[i]+=1
key_sorted = sorted(count_dict.keys())
for i in key_sorted : 
	print i,count_dict[i]

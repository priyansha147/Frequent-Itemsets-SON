from pyspark import SparkContext
import sys
import itertools
from operator import add

def global_counts(InputChunk):              #Map Phase 2
    transactionList = list(InputChunk)
    Counts = {}
    for t in transactionList:
	for candidate in ReducePhaseOne:
	   if type(candidate[0]) is tuple:
              ind = list(candidate[0])
           else:
              ind = []
              ind.append(candidate[0])
           super = map(int,t.split(","))
	   if set(ind) <= set(super):
              if candidate[0] in Counts:
	           Counts[candidate[0]] = Counts[candidate[0]] + 1
	      else:
	           Counts[candidate[0]] = 1

    mapOut = []
    for k, v in Counts.iteritems():
	mapOut.append((k,v))

    return mapOut

def trim(x, size):
    l = []
    for num in range(0, size):
        l.append(x[num][0])
    return l

def comb_generator(singles, Candidates, s, transactionList, ItemsetSize, cnt): #Generates higher order itemsets and returns frequent itemsets in Candidates list
    itemCombinations = list(itertools.combinations(singles, ItemsetSize))

    Counts = {}
    currCand = []
    for item in itemCombinations:
        temp = list(itertools.combinations(item, ItemsetSize - 1))  # Generates subsets of itemsets
        if len(temp) == 2:
            temp = trim(temp, 2)
        if set(temp) <= set(Candidates):  # Checks if all subsets are frequent
            currCand.append(item)

    for t in transactionList:
        for item in currCand:
                ind = list(itertools.combinations(item, 1))
                ind = trim(ind, ItemsetSize)
                super = map(int,t.split(","))
                if set(ind) <= set(super):          
                    if item in Counts:
                        Counts[item] = Counts[item] + 1
                    else:
                        Counts[item] = 1

    for k, v in Counts.iteritems():
        if v >= s:                   #Returns itemsets with count>support
            Candidates.append(k)
            cnt = cnt +1
    return Candidates, cnt


def frequent_singles(InputChunk):             #Generates single itemsets and pass them to a function that calculates higher order
    Candidates = []
    singles = []
    transactionList = []
    Counts = {}
    items = set()
    transactionList = list(InputChunk)
		
    for t in transactionList:
	item_list = t.split(",")
        for item in item_list:
		items.add(int(item))            #Generates a set to store all items without repitition
                if item in Counts:
                   Counts[item] = Counts[item] + 1
                else:
                   Counts[item] = 1

    s = Sup_ratio*len(transactionList)

    for k, v in Counts.iteritems():
        if v >= s:
            k = int(k)
            Candidates.append(k)
            singles.append(k)

    ItemsetSize = 1
    cnt = len(Candidates)
    c = []
    while cnt > 0:               #Calls the function till its no longer possible to find frequent itemsets
        ItemsetSize = ItemsetSize + 1
        c, cnt = comb_generator(sorted(singles), Candidates, s, transactionList, ItemsetSize, 0)

    mapOut = []
    for key in c:
        mapOut.append((key, 1))

    return mapOut


	
sc = SparkContext(appName="SON")
BasketFile = sc.textFile(sys.argv[1])
Sup_ratio = float(sys.argv[2])
Size = BasketFile.count()
Support = Size*Sup_ratio
MapPhaseOne = BasketFile.mapPartitions(frequent_singles)
ReducePhaseOne = MapPhaseOne.reduceByKey(lambda x,y : x).collect()
MapPhaseTwo = BasketFile.mapPartitions(global_counts)
ReducePhaseTwo = MapPhaseTwo.reduceByKey(add).filter(lambda x: x[1]>=Support).collect()
fo = open(sys.argv[3], "w")
for v in ReducePhaseTwo:
    if type(v[0]) is tuple:
	for val in range(0, len(v[0]) - 1):
	   fo.write(str(v[0][val]) + ",")
	fo.write(str(v[0][val+1]))
    else:
	fo.write(str(v[0]))
    fo.write("\n")
fo.close()

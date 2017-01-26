#section 4.1.2
tranFile = sc.textFile("first-edition/ch04/ch04_data_transactions.txt")
tranData = tranFile.map(lambda line: line.split("#"))
transByCust = tranData.map(lambda t: (int(t[2]), t))

transByCust.keys().distinct().count()

import operator
transByCust.countByKey()
sum(transByCust.countByKey().values())
(cid, purch) = sorted(transByCust.countByKey().items(), key=operator.itemgetter(1))[-1]
complTrans = [["2015-03-30", "11:59 PM", "53", "4", "1", "0.00"]]

transByCust.lookup(53)
for t in transByCust.lookup(53):
    print(", ".join(t))

def applyDiscount(tran):
    if(int(tran[3])==25 and float(tran[4])>1):
        tran[5] = str(float(tran[5])*0.95)
    return tran

transByCust = transByCust.mapValues(lambda t: applyDiscount(t))

def addToothbrush(tran):
    if(int(tran[3]) == 81 and int(tran[4])>4):
        from copy import copy
        cloned = copy(tran)
        cloned[5] = "0.00"
        cloned[3] = "70"
        cloned[4] = "1"
        return [tran, cloned]
    else:
        return [tran]

transByCust = transByCust.flatMapValues(lambda t: addToothbrush(t))

amounts = transByCust.mapValues(lambda t: float(t[5]))
totals = amounts.foldByKey(0, lambda p1, p2: p1 + p2).collect()
amounts.foldByKey(100000, lambda p1, p2: p1 + p2).collect()

complTrans += [["2015-03-30", "11:59 PM", "76", "63", "1", "0.00"]]
transByCust = transByCust.union(sc.parallelize(complTrans).map(lambda t: (int(t[2]), t)))
transByCust.map(lambda t: "#".join(t[1])).saveAsTextFile("ch04output-transByCust")

prods = transByCust.aggregateByKey([], lambda prods, tran: prods + [tran[3]],
    lambda prods1, prods2: prods1 + prods2)
prods.collect()


#section 4.2.2
rdd.aggregateByKey(zeroValue, seqFunc, comboFunc, 100).collect()
#in Python there is no version of aggregateByKey with a custom partitioner

rdd = sc.parallelize(range(10000))
rdd.map(lambda x: (x, x*x)).map(lambda (x, y): (y, x)).collect()
rdd.map(lambda x: (x, x*x)).reduceByKey(lambda v1, v2: v1+v2).collect()

#section 4.2.4
import random
l = [random.randrange(100) for x in range(500)]
rdd = sc.parallelize(l, 30).glom()
rdd.collect()
rdd.count()

#section 4.3.1
transByProd = transByCust.map(lambda ct: (int(ct[1][3]), ct[1]))
totalsByProd = transByProd.mapValues(lambda t: float(t[5])).reduceByKey(lambda tot1, tot2: tot1 + tot2)

products = sc.textFile("first-edition/ch04/ch04_data_products.txt").map(lambda line: line.split("#")).map(lambda p: (int(p[0]), p))
totalsAndProds = totalsByProd.join(products)
totalsAndProds.first()

totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
missingProds = totalsWithMissingProds.filter(lambda x: x[1][1] is None).map(lambda x: x[1][0])
from __future__ import print_function
missingProds.foreach(lambda p: print(", ".join(p)))


missingProds = products.subtractByKey(totalsByProd)
missingProds.foreach(lambda p: print(", ".join(p[1])))

prodTotCogroup = totalsByProd.cogroup(products)
prodTotCogroup.filter(lambda x: len(x[1][0].data) == 0).foreach(lambda x: print(", ".join(x[1][1].data[0])))
totalsAndProds = prodTotCogroup.filter(lambda x: len(x[1][0].data)>0).\
map(lambda x: (int(x[1][1].data[0][0]),(x[1][0].data[0], x[1][1].data[0])))

totalsByProd.map(lambda t: t[0]).intersection(products.map(lambda p: p[0]))

rdd1 = sc.parallelize([7,8,9])
rdd2 = sc.parallelize([1,2,3])
rdd1.cartesian(rdd2).collect()
rdd1.cartesian(rdd2).filter(lambda el: el[0] % el[1] == 0).collect()

rdd1 = sc.parallelize([1,2,3])
rdd2 = sc.parallelize([2,3,4])
rdd1.intersection(rdd2).collect()

missing = transByProd.subtractByKey(products)

rdd1 = sc.parallelize([1,2,3])
rdd2 = sc.parallelize(["n4","n5","n6"])
rdd1.zip(rdd2).collect()

#zipPartitions is not implemented in Python yet

#section 4.3.2
sortedProds = totalsAndProds.sortBy(lambda t: t[1][1][1])
sortedProds.collect()

#section 4.3.3
def createComb(t):
    total = float(t[5])
    q = int(t[4])
    return (total/q, total/q, q, total)

def mergeVal((mn,mx,c,tot),t):
    total = float(t[5])
    q = int(t[4])
    return (min(mn,total/q),max(mx,total/q),c+q,tot+total)

def mergeComb((mn1,mx1,c1,tot1),(mn2,mx2,c2,tot2)):
    return (min(mn1,mn1),max(mx1,mx2),c1+c2,tot1+tot2)

avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb).\
mapValues(lambda (mn,mx,cnt,tot): (mn,mx,cnt,tot,tot/cnt))
avgByCust.first()

totalsAndProds.map(lambda p: p[1]).map(lambda x: ", ".join(x[1])+", "+str(x[0])).saveAsTextFile("ch04output-totalsPerProd")
avgByCust.map(lambda (pid, (mn, mx, cnt, tot, avg)): "%d#%.2f#%.2f#%d#%.2f#%.2f" % (pid, mn, mx, cnt, tot, avg)).saveAsTextFile("ch04output-avgByCust")

#section 4.4.1
import random
l = [random.randrange(10) for x in range(500)]
listrdd = sc.parallelize(l, 5)
pairs = listrdd.map(lambda x: (x, x*x))
reduced = pairs.reduceByKey(lambda v1, v2: v1+v2)
finalrdd = reduced.mapPartitions(lambda itr: ["K="+str(k)+",V="+str(v) for (k,v) in itr])
finalrdd.collect()
print(finalrdd.toDebugString)

#section 4.5.1
#accumulators in Python cannot be named
acc = sc.accumulator(0)
l = sc.parallelize(range(1000000))
l.foreach(lambda x: acc.add(1))
acc.value
#exception occurs
l.foreach(lambda x: acc.value)

#accumulables are not supported in Python

#accumulableCollections are not supported in Python

################ Listings from chapter 02 ################\\


## *************** terminal ******************
## pyshell
## ************* end terminal ******************

### 2.2.2
licLines = sc.textFile("/usr/local/spark/LICENSE")
lineCnt = licLines.count()

bsdLines = licLines.filter(lambda line: "BSD" in line)
bsdLines.count()

from __future__ import print_function
bsdLines.foreach(lambda bLine: print(bLine))

def isBSD(line):
    return "BSD" in line

bsdLines1 = licLines.filter(isBSD)
bsdLines1.count()
bsdLines.foreach(lambda bLine: print(bLine))

### 2.3.1
numbers = sc.parallelize(range(10, 51, 10))
numbers.foreach(lambda x: print(x))
numbersSquared = numbers.map(lambda num: num * num)
numbersSquared.foreach(lambda x: print(x))

reversed = numbersSquared.map(lambda x: str(x)[::-1])
reversed.foreach(lambda x: print(x))

### 2.3.2

# *************** terminal ******************
# echo "15,16,20,20
# 77,80,94
# 94,98,16,31
# 31,15,20" > ~/client-ids.log
# ************* end terminal *****************

lines = sc.textFile("/home/spark/client-ids.log")

idsStr = lines.map(lambda line: line.split(","))
idsStr.foreach(lambda x: print(x))

idsStr.first()

idsStr.collect()

ids = lines.flatMap(lambda x: x.split(","))

ids.collect()

ids.first()

"; ".join(ids.collect())

intIds = ids.map(lambda x: int(x))
intIds.collect()

uniqueIds = intIds.distinct()
uniqueIds.collect()
finalCount  = uniqueIds.count()
finalCount

transactionCount = ids.count()
transactionCount

### 2.3.3

s = uniqueIds.sample(False, 0.3)
s.count()
s.collect()

swr = uniqueIds.sample(True, 0.5)
swr.count
swr.collect()

taken = uniqueIds.takeSample(False, 5)
uniqueIds.take(3)

### 2.4.1
intIds.mean()
intIds.sum()

intIds.variance()
intIds.stdev()

### 2.4.2
intIds.histogram([1.0, 50.0, 100.0])
intIds.histogram(3)


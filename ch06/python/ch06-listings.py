from __future__ import print_function
from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 5)

filestream = ssc.textFileStream("/home/spark/ch06input")

from datetime import datetime
def parseOrder(line):
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
      "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = filestream.flatMap(parseOrder)

from operator import add
numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)

numPerType.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")

ssc.start()

ssc.stop(False)

allCounts = sc.textFile("/home/spark/ch06output/output*.txt")

#section 6.1.8
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 5)
filestream = ssc.textFileStream("/home/spark/ch06input")

from datetime import datetime
def parseOrder(line):
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
      "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = filestream.flatMap(parseOrder)
from operator import add
numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)

amountPerClient = orders.map(lambda o: (o['clientId'], o['amount']*o['price']))

amountState = amountPerClient.updateStateByKey(lambda vals, totalOpt: sum(vals)+totalOpt if totalOpt != None else sum(vals))
top5clients = amountState.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))

buySellList = numPerType.map(lambda t: ("BUYS", [str(t[1])]) if t[0] else ("SELLS", [str(t[1])]) )
top5clList = top5clients.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5CLIENTS", arr))

finalStream = buySellList.union(top5clList)

finalStream.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")

sc.setCheckpointDir("/home/spark/checkpoint/")

ssc.start()

#6.1.9 window operations
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 5)
filestream = ssc.textFileStream("/home/spark/ch06input")

from datetime import datetime
def parseOrder(line):
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
      "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = filestream.flatMap(parseOrder)
from operator import add
numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)

amountPerClient = orders.map(lambda o: (o['clientId'], o['amount']*o['price']))

amountState = amountPerClient.updateStateByKey(lambda vals, totalOpt: sum(vals)+totalOpt if totalOpt != None else sum(vals))
top5clients = amountState.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))

buySellList = numPerType.map(lambda t: ("BUYS", [str(t[1])]) if t[0] else ("SELLS", [str(t[1])]) )
top5clList = top5clients.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5CLIENTS", arr))

stocksWindow = orders.map(lambda x: (x['symbol'], x['amount'])).window(60*60)
stocksPerWindow = stocksWindow.reduceByKey(add)

topStocks = stocksPerWindow.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).map(lambda x: x[0]).\
zipWithIndex().filter(lambda x: x[1] < 5)).repartition(1).\
map(lambda x: str(x[0])).glom().\
map(lambda arr: ("TOP5STOCKS", arr))

finalStream = buySellList.union(top5clList).union(topStocks)

finalStream.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")

sc.setCheckpointDir("/home/spark/checkpoint/")

ssc.start()

#6.2.2
#code for kafkaProducerWrapper
from kafka import KafkaProducer
class KafkaProducerWrapper(object):
  producer = None
  @staticmethod
  def getProducer(brokerList):
    if KafkaProducerWrapper.producer != None:
      return KafkaProducerWrapper.producer
    else:
      KafkaProducerWrapper.producer = KafkaProducer(bootstrap_servers=brokerList, key_serializer=str.encode, value_serializer=str.encode)
      return KafkaProducerWrapper.producer

#Listing 6.2
from __future__ import print_function
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaError

ssc = StreamingContext(sc, 5)

kafkaReceiverParams = {"metadata.broker.list": "192.168.10.2:9092"}
kafkaStream = KafkaUtils.createDirectStream(ssc, ["orders"], kafkaReceiverParams)

from datetime import datetime
def parseOrder(line):
  s = line[1].split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
      "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = kafkaStream.flatMap(parseOrder)
from operator import add
numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)

amountPerClient = orders.map(lambda o: (o['clientId'], o['amount']*o['price']))

amountState = amountPerClient.updateStateByKey(lambda vals, totalOpt: sum(vals)+totalOpt if totalOpt != None else sum(vals))
top5clients = amountState.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))

buySellList = numPerType.map(lambda t: ("BUYS", [str(t[1])]) if t[0] else ("SELLS", [str(t[1])]) )
top5clList = top5clients.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5CLIENTS", arr))

stocksPerWindow = orders.map(lambda x: (x['symbol'], x['amount'])).reduceByKeyAndWindow(add, None, 60*60)
stocksSorted = stocksPerWindow.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))
topStocks = stocksSorted.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5STOCKS", arr))

finalStream = buySellList.union(top5clList).union(topStocks)
finalStream.foreachRDD(lambda rdd: rdd.foreach(print))

def sendMetrics(itr):
  prod = KafkaProducerWrapper.getProducer(["192.168.10.2:9092"])
  for m in itr:
    prod.send("metrics", key=m[0], value=m[0]+","+str(m[1]))
  prod.flush()

finalStream.foreachRDD(lambda rdd: rdd.foreachPartition(sendMetrics))

sc.setCheckpointDir("/home/spark/checkpoint/")

ssc.start()

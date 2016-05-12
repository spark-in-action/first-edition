
from __future__ import print_function
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, SparkConf
import sys
from datetime import datetime
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer

class KafkaProducerWrapper(object):
		producer = None
		@staticmethod
		def getProducer(brokerList):
			if KafkaProducerWrapper.producer == None:
				KafkaProducerWrapper.producer = KafkaProducer(bootstrap_servers=brokerList, key_serializer=str.encode, value_serializer=str.encode)
			return KafkaProducerWrapper.producer

if __name__ == "__main__":
	#used for connecting to Kafka
	brokerList = ""
	#Spark checkpoint directory
	checkpointDir = ""
	#Kafka topic for reading log messages
	logsTopic = "weblogs"
	#Kafka topic for writing the calculated statistics
	statsTopic = "stats"
	#Session timeout in milliseconds
	SESSION_TIMEOUT_MILLIS = 2 * 60 * 1000 #2 minutes
	#Number of RDD partitions to use
	numberPartitions = 3

	def printUsageAndExit():
		print("Usage: StreamingLogAnalyzer.py -brokerList=<kafka_host1:port1,...> -checkpointDir=HDFS_DIR [options]\n" +
			"\n" +
			"Options:\n" +
			" -inputTopic=NAME			Input Kafka topic name for reading logs data. Default is 'weblogs'.\n" +
			" -outputTopic=NAME		 Output Kafka topic name for writing aggregated statistics. Default is 'stats'.\n" +
			" -sessionTimeout=NUM		Session timeout in minutes. Default is 2.\n" +
			" -numberPartitions=NUM Number of partitions for the streaming job. Default is 3.\n")
		sys.exit(1)

	def parseAndValidateArguments(args):
		global brokerList, checkpointDir, logsTopic, statsTopic, SESSION_TIMEOUT_MILLIS, numberPartitions
		try:
			for arg in args[1:]:
				if arg.startswith("-brokerList="):
					brokerList = arg[12:]
				elif arg.startswith("-checkpointDir="):
					checkpointDir = arg[15:]
				elif arg.startswith("-inputTopic="):
					logsTopic = arg[12:]
				elif arg.startswith("-outputTopic="):
					statsTopic = arg[13:]
				elif arg.startswith("-sessionTimeout="):
					SESSION_TIMEOUT_MILLIS = int(arg[16:]) * 60 * 1000
				elif arg.startswith("-numberPartitions="):
					numberPartitions = int(arg[18:])
				else:
					printUsageAndExit()
		except Exception as e:
			print(e)
			printUsageAndExit()

		if (brokerList == "" or checkpointDir == "" or logsTopic == "" or statsTopic == "" or SESSION_TIMEOUT_MILLIS < 60 * 1000 or numberPartitions < 1):
			printUsageAndExit()

	conf = SparkConf()
	conf.setAppName("Streaming Log Analyzer")
	sc = SparkContext(conf=conf)
	#this will exit if arguments are not valid
	parseAndValidateArguments(sys.argv)

	ssc = StreamingContext(sc, 1)

	ssc.checkpoint(checkpointDir)

	#set up the receiving Kafka stream
	print("Starting Kafka direct stream to broker list %s, input topic %s, output topic %s" % (brokerList, logsTopic, statsTopic))

	kafkaReceiverParams = {"metadata.broker.list": brokerList}
	kafkaStream = KafkaUtils.createDirectStream(ssc, [logsTopic], kafkaReceiverParams)

	def parseLine(line):
		fields = line[1].split(" ")
		try:
				return [{"time": datetime.strptime(fields[0]+" "+fields[1], "%Y-%m-%d %H:%M:%S.%f"), "ipAddr": fields[2], "sessId": fields[3], "url": fields[4],
				"method": fields[5], "respCode": int(fields[6]), "respTime": int(fields[7]) }]
		except Exception as err:
				print("Wrong line format (%s): " % line)
				return []

	logsStream = kafkaStream.flatMap(parseLine)

	#CALCULATE NUMBER OF SESSIONS
	#contains session id keys its maximum (last) timestamp as value
	maxTimeBySession = logsStream.map(lambda r: (r['sessId'], r['time']))
	maxTimeBySession = maxTimeBySession.reduceByKey(lambda t1, t2: max(t1, t2))

	#update state by session id
	def sessionize(maxTimeNewValues, maxTimeOldState):
		if(len(maxTimeNewValues) == 0): #only old session exists
			#check if the session timed out
			if ((datetime.now() - maxTimeOldState).total_seconds() * 1000 > SESSION_TIMEOUT_MILLIS):
				return None #session timed out so remove it from the state
			else:
				return maxTimeOldState #preserve the current state
		elif (maxTimeOldState == None): #this is a new session; no need to check the timeout
			return maxTimeNewValues[0] #create the new state using the new value (only one new value is possible because of the previous reduceByKey)
		else: #both old and new events with this session id found; no need to check the timeout
			return max(maxTimeNewValues[0], maxTimeOldState)

	stateBySession = maxTimeBySession.updateStateByKey(sessionize)
	#returns a DStream with single-element RDDs containing only the total count
	sessionCount = stateBySession.count()

	zerotime = datetime.utcfromtimestamp(0)

	#logLinesPerSecond contains (time, LogLine) tuples
	logLinesPerSecond = logsStream.map(lambda l: (long((l['time'] - zerotime).total_seconds() * 1000), l))

	#CALCULATE REQUESTS PER SECOND
	#this combineByKey counts all LogLines per unique second

	reqsPerSecond = logLinesPerSecond.combineByKey(lambda l: 1L, lambda c, ll: c+1, lambda c1, c2: c1 + c2, numPartitions=numberPartitions)

	#CALCULATE ERRORS PER SECOND
	errorsPerSecond = logLinesPerSecond.filter(lambda l: l[1]['respCode']/100 == 4 or l[1]['respCode']/100 == 5)
	errorsPerSecond = errorsPerSecond.combineByKey(lambda r: 1L, lambda c, r: c+1, lambda c1, c2: c1+c2, numPartitions=numberPartitions)

	#CALCULATE NUMBER OF ADS PER SECOND
	def matchAd(l):
		import re
		adUrlPattern = re.compile('.*/ads/(\d+)/\d+/clickfw')
		m = adUrlPattern.match(l[1]['url'])
		if m == None:
			return []
		else:
			return [((l[0], m.group(1)), l[1])]

	adsPerSecondAndType = logLinesPerSecond.flatMap(matchAd)
	adsPerSecondAndType = adsPerSecondAndType.combineByKey(lambda r : 1L, lambda c, r: c+1, lambda c1,c2: c1+c2, numPartitions=numberPartitions)

	#data key types for the output map
	SESSION_COUNT = "SESS"
	REQ_PER_SEC = "REQ"
	ERR_PER_SEC = "ERR"
	ADS_PER_SEC = "AD"

	requests = reqsPerSecond.map(lambda sc: (sc[0], {REQ_PER_SEC: sc[1]}))
	errors = errorsPerSecond.map(lambda sc : (sc[0], {ERR_PER_SEC: sc[1]}))
	finalSessionCount = sessionCount.map(lambda c : (long((datetime.now() - zerotime).total_seconds() * 1000), {SESSION_COUNT: c}))
	ads = adsPerSecondAndType.map(lambda stc: (stc[0][0], {ADS_PER_SEC+"#"+stc[0][1]: stc[1]}))

	#all the streams are unioned and combined
	finalStats = finalSessionCount.union(requests).union(errors).union(ads).reduceByKey(lambda m1, m2: dict(m1.items() + m2.items()))
	def sendMetrics(itr):
		global brokerList
		prod = KafkaProducerWrapper.getProducer([brokerList])
		for m in itr:
			mstr = ",".join([str(x) + "->" + str(m[1][x]) for x in m[1]])
			prod.send(statsTopic, key=str(m[0]), value=str(m[0])+":("+mstr+")")
		prod.flush()

	#Each partitions uses its own Kafka producer (one per partition) to send the formatted message
	finalStats.foreachRDD(lambda rdd: rdd.foreachPartition(sendMetrics))

	print("Starting the streaming context... Kill me with ^C")

	ssc.start()
	ssc.awaitTermination()




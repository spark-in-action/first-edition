package org.sia.loganalyzer

import org.apache.spark._
import kafka.serializer.StringDecoder
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._
import StreamingContext._
import org.apache.spark.streaming.kafka._
import java.text.SimpleDateFormat
import java.text.DateFormat
import scala.util.matching.Regex
import java.util.Properties

/**
 * Spark Streaming application for analyzing access logs coming as text messages from a Kafka topic,
 * calculating various statistics and writing the results back to Kafka, to a different topic.
 *
 * When submitting the application, two parameters are required:
 *     -brokerList=<kafka_host1:port1,...> - comma-separated list of Kafka broker host names and port numbers
 *     -checkpointDir=HDFS_DIR             - URL to an HDFS directory to be used for storing Spark checkpoint data
 * Other optional parameters:
 *     -inputTopic=NAME        Input Kafka topic name for reading logs data. Default is 'weblogs'.
 *     -outputTopic=NAME       Output Kafka topic name for writing aggregated statistics. Default is 'stats'.
 *     -sessionTimeout=NUM     Session timeout in minutes. Default is 2.
 *     -numberPartitions=NUM   Number of partitions for the streaming job. Default is 3.
 *
 * The incoming messages should be in this format:
 * <yyyy-MM-dd> <hh:mm:ss.SSS> <client IP address> <client session ID> <URL visited> <HTTP method> <response code> <response duration>
 *
 * The statistics calculated are:
 *     - number of active sessions (those sessions whose last request happened less than sessionTimeout seconds ago).
 *     - number of requests per second
 *     - number of errors per second
 *     - number of ads per second
 *
 *  All timestamps are rounded (floored) to whole seconds (milliseconds are erased) and the log events are grouped by second.
 *
 *  Statistics are written back to Kafka in the following format:
 *  <timestamp>:(key->value,key->value,...)
 *  Key is '1' for active sessions, '2' for requests, '3' for errors and '4#<category>' for ads, where <category> is parsed from the URL.
 *  Ad URLs are assumed to be in the format "/ads/<ad_category>/<ad_id>/clickfw".
 */
object StreamingLogAnalyzer
{
  //used for connecting to Kafka
  var brokerList: Option[String] = None
  //Spark checkpoint directory
  var checkpointDir: Option[String] = None
  //Kafka topic for reading log messages
  var logsTopic: Option[String] = Some("weblogs")
  //Kafka topic for writing the calculated statistics
  var statsTopic: Option[String] = Some("stats")
  //Session timeout in milliseconds
  var SESSION_TIMEOUT_MILLIS = 2 * 60 * 1000 //2 minutes
  //Number of RDD partitions to use
  var numberPartitions = 3

  def main(args: Array[String])
  {
    //this will exit if arguments are not valid
    parseAndValidateArguments(args)

    val conf = new SparkConf().setAppName("Streaming Log Analyzer")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint(checkpointDir.get)

    //set up the receiving Kafka stream
    println("Starting Kafka direct stream to broker list: "+brokerList.get)
    val kafkaReceiverParams = Map[String, String](
        "metadata.broker.list" -> brokerList.get)
    val kafkaStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaReceiverParams, Set(logsTopic.get))

    //case class for storing the contents of each access log line
    case class LogLine(time: Long, ipAddr: String, sessId: String, url: String, method: String, respCode: Int, respTime: Int)

    val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

    //logStream contains parsed LogLines
    val logsStream = kafkaStream.flatMap { t =>
      {
        val fields = t._2.split(" ")
        try {
          List(LogLine(df.parse(fields(0) + " " + fields(1)).getTime(), fields(2), fields(3), fields(4), fields(5), fields(6).toInt, fields(7).toInt))
        }
        catch {
          case e: Exception => { System.err.println("Wrong line format: "+t); List() }
        }
      }
    }//flatMap

    //CALCULATE NUMBER OF SESSIONS
    //contains session id keys its maximum (last) timestamp as value
    val maxTimeBySession = logsStream.map(r => (r.sessId, r.time)).reduceByKey(
      (max1, max2) => {
        Math.max(max1, max2)
      })//reduceByKey
    //update state by session id
    val stateBySession = maxTimeBySession.updateStateByKey((maxTimeNewValues: Seq[Long], maxTimeOldState: Option[Long]) => {
      if (maxTimeNewValues.size == 0) //only old session exists
      {
        //check if the session timed out
        if (System.currentTimeMillis() - maxTimeOldState.get > SESSION_TIMEOUT_MILLIS)
          None //session timed out so remove it from the state
        else
          maxTimeOldState //preserve the current state
      }
      else if (maxTimeOldState.isEmpty) //this is a new session; no need to check the timeout
        Some(maxTimeNewValues(0)) //create the new state using the new value (only one new value is possible because of the previous reduceByKey)
      else //both old and new events with this session id found; no need to check the timeout
        Some(Math.max(maxTimeNewValues(0), maxTimeOldState.get))
    })//updateStateByKey
    //returns a DStream with single-element RDDs containing only the total count
    val sessionCount = stateBySession.count()

    //logLinesPerSecond contains (time, LogLine) tuples
    val logLinesPerSecond = logsStream.map(l => ((l.time / 1000) * 1000, l))

    //CALCULATE REQUESTS PER SECOND
    //this combineByKey counts all LogLines per unique second
    val reqsPerSecond = logLinesPerSecond.combineByKey(
        l => 1L,
        (c: Long, ll: LogLine) => c + 1,
        (c1: Long, c2: Long) => c1 + c2,
        new HashPartitioner(numberPartitions),
        true)

    //CALCULATE ERRORS PER SECOND
    val errorsPerSecond = logLinesPerSecond.
      //leaves in only the LogLines with response code starting with 4 or 5
      filter(l => { val respCode = l._2.respCode / 100; respCode == 4 || respCode == 5 }).
      //this combineByKey counts all LogLines per unique second
      combineByKey(r => 1L,
          (c: Long, r: LogLine) => c + 1,
          (c1: Long, c2: Long) => c1 + c2,
          new HashPartitioner(numberPartitions),
          true)

    //CALCULATE NUMBER OF ADS PER SECOND
    val adUrlPattern = new Regex(".*/ads/(\\d+)/\\d+/clickfw", "adtype")
    val adsPerSecondAndType = logLinesPerSecond.
      //filters out the LogLines whose URL's don't match the adUrlPattern.
      //LogLines that do match the adUrlPattern are mapped to tuples ((timestamp, parsed ad category), LogLine)
      flatMap(l => {
        adUrlPattern.findFirstMatchIn(l._2.url) match {
          case Some(urlmatch) => List(((l._1, urlmatch.group("adtype")), l._2))
          case None => List()
        }
      }).
      //this combineByKey counts all LogLines per timestamp and ad category
      combineByKey(r => 1.asInstanceOf[Long],
          (c: Long, r: LogLine) => c + 1,
          (c1: Long, c2: Long) => c1 + c2,
          new HashPartitioner(numberPartitions),
          true)

    //data key types for the output map
    val SESSION_COUNT = "SESS"
    val REQ_PER_SEC = "REQ"
    val ERR_PER_SEC = "ERR"
    val ADS_PER_SEC = "AD"

    //maps each count to a tuple (timestamp, a Map containing the count under the REQ_PER_SEC key)
    val requests = reqsPerSecond.map(sc => (sc._1, Map(REQ_PER_SEC -> sc._2)))
    //maps each count to a tuple (timestamp, a Map containing the count under the ERR_PER_SEC key)
    val errors = errorsPerSecond.map(sc => (sc._1, Map(ERR_PER_SEC -> sc._2)))
    //maps each count to a tuple (current time with milliseconds removed, a Map containing the count under the SESSION_COUNT key)
    val finalSessionCount = sessionCount.map(c => ((System.currentTimeMillis / 1000) * 1000, Map(SESSION_COUNT -> c)))
    //maps each count to a tuple (timestamp, a Map containing the count per category under the key ADS_PER_SEC#<ad category>)
    val ads = adsPerSecondAndType.map(stc => (stc._1._1, Map(s"$ADS_PER_SEC#${stc._1._2}" -> stc._2)))

    //all the streams are unioned and combined
    val finalStats = finalSessionCount.union(requests).union(errors).union(ads).
      //and all the Maps containing particular counts are combined into one Map per timestamp.
      //This one Map contains all counts under their keys (SESSION_COUNT, REQ_PER_SEC, ERR_PER_SEC, etc.).
      reduceByKey((m1, m2) => m1 ++ m2)

    //Each partitions uses its own Kafka producer (one per partition) to send the formatted message
    finalStats.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          KafkaProducerWrapper.brokerList = brokerList.get
          val producer = KafkaProducerWrapper.instance
          partition.foreach {
            case (s, map) =>
              producer.send(
                  statsTopic.get,
                  s.toString,
                  s.toString + ":(" + map.foldLeft(new Array[String](0)) { case (x, y) => { x :+ y._1 + "->" + y._2 } }.mkString(",") + ")")
          }//foreach
        })//foreachPartition
      })//foreachRDD

    println("Starting the streaming context... Kill me with ^C")

    ssc.start()
    ssc.awaitTermination()
  }//main

  private def parseAndValidateArguments(args: Array[String]) {
    args.foreach(arg => {
      arg match {
        case bl if bl.startsWith("-brokerList=") =>
          brokerList = Some(bl.substring(12))
        case st if st.startsWith("-checkpointDir=") =>
          checkpointDir = Some(st.substring(15))
        case st if st.startsWith("-inputTopic=") =>
          logsTopic = Some(st.substring(12))
        case st if st.startsWith("-outputTopic=") =>
          statsTopic = Some(st.substring(13))
        case st if st.startsWith("-sessionTimeout=") =>
          SESSION_TIMEOUT_MILLIS = parseInt(st.substring(16)) * 60 * 1000
        case np if np.startsWith("-numberPartitions=") =>
          numberPartitions = parseInt(np.substring(18))
        case _ =>
          printUsageAndExit()
      }
    })

    if (brokerList.isEmpty || checkpointDir.isEmpty || logsTopic.isEmpty || statsTopic.isEmpty || SESSION_TIMEOUT_MILLIS < 60 * 1000 || numberPartitions < 1)
      printUsageAndExit()
  } //parseAndValidateArguments

  private def parseInt(str: String) = try {
    str.toInt
  } catch {
    case e: NumberFormatException => { printUsageAndExit(); 0 }
  }

  private def printUsageAndExit() {
    System.err.println("Usage: StreamingLogAnalyzer -brokerList=<kafka_host1:port1,...> -checkpointDir=HDFS_DIR [options]\n" +
      "\n" +
      "Options:\n" +
      "  -inputTopic=NAME        Input Kafka topic name for reading logs data. Default is 'weblogs'.\n" +
      "  -outputTopic=NAME       Output Kafka topic name for writing aggregated statistics. Default is 'stats'.\n" +
      "  -sessionTimeout=NUM     Session timeout in minutes. Default is 2.\n" +
      "  -numberPartitions=NUM   Number of partitions for the streaming job. Default is 3.\n")
    System.exit(1)
  }
}//StreamingLogAnalyzer

/**
 * Case class and the matching object for lazily initializing a Kafka Producer.
 */
case class KafkaProducerWrapper(brokerList: String)
{
  val producerProps = {
    val prop = new Properties
    prop.put("metadata.broker.list", brokerList)
    prop
  }
  val p = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerProps))

  def send(topic: String, key: String, value: String) {
    p.send(new KeyedMessage(topic, key.toCharArray.map(_.toByte), value.toCharArray.map(_.toByte)))
  }
}

object KafkaProducerWrapper {
  var brokerList = ""
  lazy val instance = new KafkaProducerWrapper(brokerList)
}


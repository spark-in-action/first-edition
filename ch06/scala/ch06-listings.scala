import org.apache.spark._
import org.apache.spark.streaming._

val ssc = new StreamingContext(sc, Seconds(5))

val filestream = ssc.textFileStream("/home/spark/ch06input")

import java.sql.Timestamp
case class Order(time: java.sql.Timestamp, orderId:Long, clientId:Long, symbol:String, amount:Int, price:Double, buy:Boolean)

import java.text.SimpleDateFormat
val orders = filestream.flatMap(line => {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val s = line.split(",")
    try {
        assert(s(6) == "B" || s(6) == "S")
        List(Order(new Timestamp(dateFormat.parse(s(0)).getTime()), s(1).toLong, s(2).toLong, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
    }
    catch {
        case e : Throwable => println("Wrong line format ("+e+"): "+line)
        List()
    }
})

val numPerType = orders.map(o => (o.buy, 1L)).reduceByKey((c1, c2) => c1+c2)

numPerType.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")

ssc.start()

ssc.stop(false)

val allCounts = sc.textFile("/home/spark/ch06output/output*.txt")


//section 6.1.8
import org.apache.spark._
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(5))

val filestream = ssc.textFileStream("/home/spark/ch06input")

import java.sql.Timestamp
case class Order(time: java.sql.Timestamp, orderId:Long, clientId:Long, symbol:String, amount:Int, price:Double, buy:Boolean)

import java.text.SimpleDateFormat
val orders = filestream.flatMap(line => {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val s = line.split(",")
    try {
        assert(s(6) == "B" || s(6) == "S")
        List(Order(new Timestamp(dateFormat.parse(s(0)).getTime()), s(1).toLong, s(2).toLong, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
    }
    catch {
        case e : Throwable => println("Wrong line format ("+e+"): "+line)
        List()
    }
})

val numPerType = orders.map(o => (o.buy, 1L)).reduceByKey((c1, c2) => c1+c2)

val amountPerClient = orders.map(o => (o.clientId, o.amount*o.price))

val amountState = amountPerClient.updateStateByKey((vals, totalOpt:Option[Double]) => {
  totalOpt match {
    case Some(total) => Some(vals.sum + total)
    case None => Some(vals.sum)
  }
})
val top5clients = amountState.transform(_.sortBy(_._2, false).map(_._1).
  zipWithIndex.filter(x => x._2 < 5))

val buySellList = numPerType.map(t =>
    if(t._1) ("BUYS", List(t._2.toString))
    else ("SELLS", List(t._2.toString)) )
val top5clList = top5clients.repartition(1).
    map(x => x._1.toString).
    glom().
    map(arr => ("TOP5CLIENTS", arr.toList))

val finalStream = buySellList.union(top5clList)

finalStream.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")

sc.setCheckpointDir("/home/spark/checkpoint/")

ssc.start()

//mapWithState
val updateAmountState = (time:Time, clientId:Long, amount:Option[Double], state:State[Double]) => {
    var total = amount.getOrElse(0.toDouble)
    if(state.exists())
        total += state.get()
    state.update(total)
    Some((clientId, total))
}
val amountState = amountPerClient.mapWithState(StateSpec.function(updateAmountState)).stateSnapshots()

//6.1.9 window operations
val stocksPerWindow = orders.map(x => (x.symbol, x.amount)).window(Minutes(60)).
  reduceByKey((a1:Int, a2:Int) => a1+a2)

val topStocks = stocksPerWindow.transform(_.sortBy(_._2, false).map(_._1).
  zipWithIndex.filter(x => x._2 < 5)).repartition(1).
    map(x => x._1.toString).glom().
    map(arr => ("TOP5STOCKS", arr.toList))

val finalStream = buySellList.union(top5clList).union(topStocks)

//6.2.2
//code for kafkaProducerWrapper.jar
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
case class KafkaProducerWrapper(brokerList: String) {
  val producerProps = {
    val prop = new java.util.Properties
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


//Listing 6.2
import org.apache.spark._
import kafka.serializer.StringDecoder
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

val ssc = new StreamingContext(sc, Seconds(5))

val kafkaReceiverParams = Map[String, String](
        "metadata.broker.list" -> "192.168.10.2:9092")
val kafkaStream = KafkaUtils.
  createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaReceiverParams, Set("orders"))

import java.sql.Timestamp
case class Order(time: java.sql.Timestamp, orderId:Long, clientId:Long, symbol:String, amount:Int, price:Double, buy:Boolean)
import java.text.SimpleDateFormat
val orders = kafkaStream.flatMap(line => {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val s = line._2.split(",")
    try {
        assert(s(6) == "B" || s(6) == "S")
        List(Order(new Timestamp(dateFormat.parse(s(0)).getTime()), s(1).toLong, s(2).toLong, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
    }
    catch {
        case e : Throwable => println("Wrong line format ("+e+"): "+line._2)
        List()
    }
})
val numPerType = orders.map(o => (o.buy, 1L)).reduceByKey((c1, c2) => c1+c2)
val amountPerClient = orders.map(o => (o.clientId, o.amount*o.price))
val amountState = amountPerClient.updateStateByKey((vals, totalOpt:Option[Double]) => {
  totalOpt match {
    case Some(total) => Some(vals.sum + total)
    case None => Some(vals.sum)
  }
})
val top5clients = amountState.transform(_.sortBy(_._2, false).map(_._1).
  zipWithIndex.filter(x => x._2 < 5))

val buySellList = numPerType.map(t =>
    if(t._1) ("BUYS", List(t._2.toString))
    else ("SELLS", List(t._2.toString)) )
val top5clList = top5clients.repartition(1).
    map(x => x._1.toString).
    glom().
    map(arr => ("TOP5CLIENTS", arr.toList))

val stocksPerWindow = orders.map(x => (x.symbol, x.amount)).reduceByKeyAndWindow((a1:Int, a2:Int) => a1+a2, Minutes(60))
val topStocks = stocksPerWindow.transform(_.sortBy(_._2, false).map(_._1).
  zipWithIndex.filter(x => x._2 < 5)).repartition(1).
    map(x => x._1.toString).glom().
    map(arr => ("TOP5STOCKS", arr.toList))

val finalStream = buySellList.union(top5clList).union(topStocks)

import org.sia.KafkaProducerWrapper
finalStream.foreachRDD((rdd) => {
  rdd.foreachPartition((iter) => {
    KafkaProducerWrapper.brokerList = "192.168.10.2:9092"
    val producer = KafkaProducerWrapper.instance
    iter.foreach({ case (metric, list) => producer.send("metrics", metric, metric + ", " + list.toString) })
  })
})

sc.setCheckpointDir("/home/spark/checkpoint/")
ssc.start()

// section 6.4
import spark.implicits._
val structStream = spark.readStream.text("ch06input")
italianPostsStream.isStreaming

import org.apache.spark.sql.streaming.ProcessingTime
val streamHandle = structStream.writeStream.format("console").trigger(ProcessingTime.create("5 seconds")).start()
streamHandle.isActive()
streamHandle.exception()
streamHandle.stop()

spark.streams.active


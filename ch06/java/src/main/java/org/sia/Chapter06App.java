package org.sia;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class Chapter06App 
{
	public static void main(String[] args) throws IOException 
	{
	    JavaSparkContext sc = new JavaSparkContext(new SparkConf());
	    
	    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

	    Map<String,String> kafkaReceiverParams = new HashMap<String,String>();
	    kafkaReceiverParams.put("metadata.broker.list", "192.168.10.2:9092");
	    Set<String> topics = new HashSet<String>();
	    topics.add("orders");
	    JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaReceiverParams, topics);
	    JavaDStream<String> inputStream = kafkaStream.map((Tuple2<String, String> t) -> t._2);
	    //to rather read from a file uncomment this:
	    //JavaDStream<String> inputStream = ssc.textFileStream("/home/spark/ch06input");
  	   
	    JavaDStream<Order> orders = inputStream.flatMap((String line) -> {
	        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	        String[] s = line.split(",");
	        try {
	            assert(s[6] == "B" || s[6] == "S");
	            List<Order> ret = new ArrayList<Order>();
	            ret.add(new Order(new Timestamp(dateFormat.parse(s[0]).getTime()), Long.parseLong(s[1]), Long.parseLong(s[2]), 
	            		s[3], Integer.parseInt(s[4]), Double.parseDouble(s[5]), s[6] == "B"));
	            return ret;
	        }
	        catch(Throwable e) {
	            System.out.println("Wrong line format ("+e+"): "+line);
	            return new ArrayList<Order>();
	        }
	    });
	    JavaPairDStream<Boolean, Long> numPerType = orders.mapToPair((Order o) -> new Tuple2<Boolean, Long>(o.buy, 1L)).reduceByKey((Long c1, Long c2) -> c1 + c2);
	    JavaPairDStream<Long, Double> amountPerClient = orders.mapToPair((Order o) -> new Tuple2<Long, Double>(o.clientId, o.amount * o.price));
	    JavaPairDStream<Long, Double> amountState = amountPerClient.updateStateByKey((List<Double> vals, Optional<Double> totalOpt) -> {
	    	double sum = vals.stream().mapToDouble(Double::doubleValue).sum();
	    	if(totalOpt.isPresent())
	    		return Optional.of(totalOpt.get() + sum);
	    	else
	    		return Optional.of(sum);
	    });
	    JavaPairDStream<Long, Long> top5clients = amountState.transformToPair((JavaPairRDD<Long, Double> rdd) -> {
	    	JavaPairRDD<Double, Long> retrdd = rdd.mapToPair((Tuple2<Long, Double> t) -> t.swap()).sortByKey(false);
	    	JavaPairRDD<Long, Long> retrdd1 = retrdd.map((Tuple2<Double, Long> t) -> t._2).zipWithIndex().filter((Tuple2<Long, Long> x) -> x._2 < 5);
	    	return retrdd1;
	      });

	    JavaPairDStream<String, List<String>> buySellList = numPerType.mapToPair((Tuple2<Boolean, Long> t) -> {
	    	List<String> ret = new ArrayList<String>();
	    	ret.add(t._2.toString());
	        if(t._1) 
	        	return new Tuple2<String, List<String>>("BUYS", ret);
	        else 
	        	return new Tuple2<String, List<String>>("SELLS", ret);
	        });
	    JavaPairDStream<String, List<String>> top5clList = top5clients.repartition(1).
	        map((Tuple2<Long, Long> x) -> x._1.toString()).
	        glom().
	        mapToPair((List<String> arr) -> new Tuple2<String, List<String>>("TOP5CLIENTS", arr));

	    JavaPairDStream<String, Integer> stocksPerWindow = orders.mapToPair((Order x) -> new Tuple2<String, Integer>(x.symbol, x.amount)).
	    	reduceByKeyAndWindow((Integer a1, Integer a2) -> a1+a2, Durations.minutes(60));
	    JavaPairDStream<String, List<String>> topStocks = stocksPerWindow.transformToPair((JavaPairRDD<String, Integer> rdd) -> rdd.
	    		mapToPair((Tuple2<String, Integer> t) -> t.swap()).sortByKey(false).
	    		map((Tuple2<Integer, String> t) -> t._2).
	    		zipWithIndex().filter((Tuple2<String, Long> x) -> x._2 < 5)).
	    	repartition(1).
	        map((Tuple2<String, Long> x) -> x._1.toString()).glom().
	        mapToPair((List<String> arr) -> new Tuple2<String, List<String>>("TOP5STOCKS", arr));

	    JavaPairDStream<String, List<String>> finalStream = buySellList.union(top5clList).union(topStocks);

	    finalStream.foreachRDD((JavaPairRDD<String, List<String>> rdd) -> {
	      rdd.foreachPartition((Iterator<Tuple2<String, List<String>>> iter) -> {
	        KafkaProducerWrapper.brokerList = "192.168.10.2:9092";
	        KafkaProducerWrapper producer = KafkaProducerWrapper.getInstance();
	        while(iter.hasNext())
	        {
	        	Tuple2<String, List<String>> t = iter.next();
	        	
	        	producer.send("metrics", t._1, t._1 + ", (" + String.join(", ", t._2) + ")");
	        }
	      });
	    });
	    //to save results to a file uncomment this:
	    //finalStream.repartition(1).saveAsNewAPIHadoopFiles("/home/spark/ch06output/output", "txt");

	    sc.setCheckpointDir("/home/spark/checkpoint/");

	    ssc.start();
	    
	    ssc.awaitTermination();
	}
	
	private static class Order {
		public Order(Timestamp time, Long orderId, Long clientId, String symbol, Integer amount, Double price, Boolean buy) {
			this.time = time;
			this.orderId = orderId;
			this.clientId = clientId;
			this.symbol = symbol;
			this.amount = amount;
			this.price = price;
			this.buy = buy;
		}
		
		Timestamp time;
		Long orderId;
		Long clientId;
		String symbol;
		Integer amount;
		Double price;
		Boolean buy;
		public Timestamp getTime() {
			return time;
		}
		public void setTime(Timestamp time) {
			this.time = time;
		}
		public Long getOrderId() {
			return orderId;
		}
		public void setOrderId(Long orderId) {
			this.orderId = orderId;
		}
		public Long getClientId() {
			return clientId;
		}
		public void setClientId(Long clientId) {
			this.clientId = clientId;
		}
		public String getSymbol() {
			return symbol;
		}
		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}
		public Integer getAmount() {
			return amount;
		}
		public void setAmount(Integer amount) {
			this.amount = amount;
		}
		public Double getPrice() {
			return price;
		}
		public void setPrice(Double price) {
			this.price = price;
		}
		public Boolean getBuy() {
			return buy;
		}
		public void setBuy(Boolean buy) {
			this.buy = buy;
		}
		
	}
}

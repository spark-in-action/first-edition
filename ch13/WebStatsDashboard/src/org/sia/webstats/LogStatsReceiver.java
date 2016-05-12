package org.sia.webstats;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * A thread for running a Kafka consumer, receiving messages and forwarding them to
 * registered LogStatsObservers.
 *
 * Static methods enable LogStatsReceiver to function as a singleton and to register
 * and deregister LogStatsObservers. When the last LogStatsObserver deregisters
 * LogStatsReceiver stops receiving messages and resumes when the first LogStatsObserver
 * registers again.
 *
 */
public class LogStatsReceiver extends Thread
{
	private static LogStatsReceiver instance;
	private static Set<LogStatsObserver> listeners = Collections.synchronizedSet(new HashSet<LogStatsObserver>());

	private static void startUp()
	{
		System.out.println("LogStatsReceiver startUp. instance: "+instance);
		if(instance == null)
		{
			instance = new LogStatsReceiver();
			instance.start();
		}
	}

	private static void shutdown()
	{
		System.out.println("LogStatsReceiver shutdown. instance: "+instance);
		if(instance != null)
		{
			instance.pleaseStop();
			instance = null;
		}
	}

	public static void addObserver(LogStatsObserver o)
	{
		System.out.println("LogStatsReceiver addObserver "+o);
		if(listeners.add(o))
			startUp();
	}

	public static void removeObserver(LogStatsObserver o)
	{
		System.out.println("LogStatsReceiver removeObserver "+o);
		listeners.remove(o);
		if(listeners.isEmpty())
			shutdown();
	}

	private AtomicBoolean shouldStop = new AtomicBoolean(false);

	public void pleaseStop()
	{
		shouldStop.set(true);
	}

	private ConsumerConnector consumer;

	public void run()
	{
		try {
			System.out.println("Starting LogStatsReceiver thread");

			String zkaddress = System.getProperty("zookeeper.address");

			if(zkaddress == null)
			{
				System.err.println("zookeeper.address property is not set! Exiting.");
				return;
			}

			String topicName = System.getProperty("kafka.topic");

			if(topicName == null)
			{
				System.err.println("kafka.topic property is not set! Exiting.");
				return;
			}

			System.out.println("LogStatsReceiver params: "+zkaddress+", "+topicName);

			Properties props = new Properties();
	        props.put("zookeeper.connect", zkaddress);
	        props.put("group.id", "groupname");
	        props.put("zookeeper.session.timeout.ms", "2400");
	        props.put("zookeeper.sync.time.ms", "1200");
	        props.put("auto.commit.interval.ms", "1000");

	        ConsumerConfig config = new ConsumerConfig(props);

	        System.out.println("LogStatsReceiver getting consumer");

	        try {
	        	consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	        }
	        catch(Exception e)
	        {
	        	e.printStackTrace();
	        	listeners.clear();
	        	instance = null;
	        	return;
	        }

	        System.out.println("LogStatsReceiver getting KafkaStream");

			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(topicName, new Integer(1));
	        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	        KafkaStream<byte[], byte[]> stream = consumerMap.get(topicName).get(0);

	        System.out.println("LogStatsReceiver iterating");

	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while (!shouldStop.get() && it.hasNext())
	        {
	        	String message = new String(it.next().message());
	        	for(LogStatsObserver listener : listeners)
	        	{
	        		listener.onStatsMessage(message);
	        	}
	        }
	        System.out.println("Stopping LogStatsReceiver.");
	        consumer.shutdown();
	        System.out.println("LogStatsReceiver stopped.");
		}
		catch(Exception e)
		{
			System.err.println("Exception during LogStatsReceiver run: ");
			e.printStackTrace();
		}
	}
}

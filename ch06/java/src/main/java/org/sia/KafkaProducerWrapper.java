package org.sia;

import kafka.javaapi.producer.Producer;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerWrapper 
{
	public static String brokerList = "";
	private static KafkaProducerWrapper instance;
	
	Properties producerProps = new Properties();
	{
		producerProps.put("metadata.broker.list", brokerList);
	}
	Producer<byte[], byte[]> p = new Producer<byte[], byte[]>(new ProducerConfig(producerProps));

	public void send(String topic, String key, String value) {
		
		p.send(new KeyedMessage<byte[], byte[]>(topic, key.getBytes(), value.getBytes()));
	}
	
	public static KafkaProducerWrapper getInstance()
	{
		if(instance == null)
			instance = new KafkaProducerWrapper();
		return instance;
	}
}

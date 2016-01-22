package org.sia.logsimulator;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Class implementing Kafka Partitioner interface. If the key is an IP address,
 * the partitioner partitions by the lowest number in the address. 
 * 
 */
public class IPPartitioner implements Partitioner
{
	public IPPartitioner(VerifiableProperties props)
	{
	}

	public int partition(Object key, int a_numPartitions)
	{
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0)
		{
			partition = Integer.parseInt(stringKey.substring(offset + 1)) % a_numPartitions;
		}
		return partition;
	}
}

package org.sia.webstats;

/**
 * 
 * Interface used for registering to receive new statistic messages from LogStatsReceiver.
 *
 */
public interface LogStatsObserver
{
	public void onStatsMessage(String message);
}

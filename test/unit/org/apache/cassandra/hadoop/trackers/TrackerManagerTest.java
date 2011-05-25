package org.apache.cassandra.hadoop.trackers;

import junitx.framework.Assert;

import org.apache.cassandra.AbstractBriskBaseTest;
import org.junit.Test;

public class TrackerManagerTest extends AbstractBriskBaseTest {
	
	@Test
	public void testReadWriteTrackerInfo() throws Exception {
		String current = TrackerManager.getCurrentJobtrackerLocation();
		
		Assert.assertNull("The DB should be empty the first time", current);
		
		TrackerManager.insertJobtrackerLocation("123.456.789.123");
		
		String newTracker = TrackerManager.getCurrentJobtrackerLocation();
		
		Assert.assertEquals("123.456.789.123", newTracker);
	}

}

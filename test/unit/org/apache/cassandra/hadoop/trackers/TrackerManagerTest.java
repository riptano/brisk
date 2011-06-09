package org.apache.cassandra.hadoop.trackers;

import java.net.InetAddress;

import junitx.framework.Assert;

import org.apache.cassandra.AbstractBriskBaseTest;
import org.junit.Test;

public class TrackerManagerTest extends AbstractBriskBaseTest {

    @Test
    public void testReadWriteTrackerInfo() throws Exception {
        InetAddress current = TrackerManager.getCurrentJobtrackerLocation();


        TrackerManager.insertJobtrackerLocation(InetAddress.getByName("127.0.1.1"));

        InetAddress newTracker = TrackerManager.getCurrentJobtrackerLocation();

        Assert.assertEquals("127.0.1.1", newTracker.getHostAddress());
    }

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop.trackers;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.log4j.Logger;

import com.datastax.brisk.BriskSchema;

//Will start job and or task trackers
//depending on the ring
public class TrackerInitializer {
    private static Logger logger = Logger.getLogger(TrackerInitializer.class);
    private static CountDownLatch jobTrackerStarted = new CountDownLatch(1);
    public static final String trackersProperty = "hadoop-trackers";
    public static final boolean isTrackerNode = System.getProperty(trackersProperty, "false").equalsIgnoreCase("true");

    // Hold the reference to the taskTracker and JobTracker thread.
    /** This attribute will be null if we are not the job tracker */
    public static Thread jobTrackerThread;
    public static Thread taskTrackerThread;
    private static TaskTracker taskTracker;
    private static ObjectName taskTrackerMBean;

    private static InetAddress lastKnowJobTracker;

    public static void init() {
        // Wait for gossip
        try {
            logger.info("Waiting for gossip to start");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        checkCreateSystemSchema();

        // Let's try until the JT location is available.
        do 
        {
            try 
            {
                InetAddress jobTrackerAddr = CassandraJobConf.getJobTrackerNode();
                lastKnowJobTracker = jobTrackerAddr;
            } catch (Exception e)
            {
                logger.info("JobTracker location not available. Retrying in 5 secs...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {}
            }
        } while (lastKnowJobTracker == null);

        launchTrackers();

        // Launches cron service to listen to JobTracker changes
        ScheduledExecutorService trackersWatcher = Executors.newSingleThreadScheduledExecutor();
        trackersWatcher.scheduleWithFixedDelay(new TrackerWatcherTask(), 60, 20, TimeUnit.SECONDS);
    }

    /**
     * Starts JobTracker(if corresponds) and the TastTrackers
     * 
     * @param jobTrackerAddr
     */
    private static void launchTrackers() {
        if (amIJobTracker()) {
            jobTrackerThread = getJobTrackerThread();
            jobTrackerThread.start();

            try {
                jobTrackerStarted.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException("JobTracker not started", e);
            }

        } else {
            if (logger.isDebugEnabled())
                logger.debug("We are not the job tracker: " + lastKnowJobTracker + " vs " + FBUtilities.getLocalAddress());
        }

        taskTrackerThread = getTaskTrackerThread();
        taskTrackerThread.start();

    }

    /**
     * In charge to detect JobTracker changes and restart the task trackers as
     * well as identify when I JobTracker in this node needs to be shutdown or
     * created.
     * 
     */
    private static class TrackerWatcherTask implements Runnable {

        @Override
        public void run() {
            // Are we a JobTracker?
            InetAddress currentJobTrackerAddr = CassandraJobConf.getJobTrackerNode();

            // Did JobTracker change?
            if (lastKnowJobTracker.equals(currentJobTrackerAddr)) {
                // Nothing changed. JobTracker is the same so the
                // Task trackers are pointing to the right place.
                return;
            }
            
            // The Job Tracker location changed. then update last know location.
            lastKnowJobTracker = currentJobTrackerAddr;

            try {
                restartTrackers();
            } catch (Exception e) {
                logger.error("Unable to restart trackers", e);
            }
        }
    }

    private static void restartTrackers() throws InterruptedException, IOException {
        reset();
        stopJobTracker();
        stopTaskTracker();
        launchTrackers();
    }

    public static void stopJobTracker() throws InterruptedException {
        // I was the JobTracker as the thread was created.
        // We need to stop it and clear the reference for future usage
        // NULL means that the JT was not launched.
        if (jobTrackerThread != null) {
            // Let the thread stop by itself
            jobTrackerThread.interrupt();
            jobTrackerThread.join(60000);
            //if (jobTrackerThread.isAlive();
            jobTrackerThread = null;
        }
    }

    public static void stopTaskTracker() throws InterruptedException, IOException {
        MBeans.unregister(taskTrackerMBean);
        taskTrackerThread.interrupt();
        taskTracker.shutdown();
        taskTrackerThread.join(60000);
    }

    /** Performs the necessary reset of resources for a restart to take place */
    private static void reset() {
        jobTrackerStarted = new CountDownLatch(1);
    }

    private static boolean amIJobTracker() {
        return lastKnowJobTracker.equals(FBUtilities.getLocalAddress());
    }

    private static void checkCreateSystemSchema() {
        try {
            BriskSchema.init();
            BriskSchema.createKeySpace();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create Brisk system schema", e);
        }
    }

    /**
     * The Job Tracker Thread.
     */
    private static Thread getJobTrackerThread() {
        Thread jobTrackerThread = new Thread(new Runnable() {

            public void run() {
                JobTracker jobTracker = null;

                while (true) {
                    try {
                        jobTracker = JobTracker.startTracker(new CassandraJobConf());
                        logger.info("Hadoop Job Tracker Started...");
                        jobTrackerStarted.countDown();
                        jobTracker.offerService();

                    } catch (Throwable t) {
                        if (t instanceof InterruptedException) {
                            try {
                                jobTracker.stopTracker();
                                logger.info("Job Tracker shutdown property");
                            } catch (Exception e) {
                                logger.error("An Error occured when stopping Job tracker");
                            }
                        }

                        // on OOM shut down the tracker
                        if (t instanceof OutOfMemoryError || t.getCause() instanceof OutOfMemoryError) {
                            try {
                                jobTracker.stopTracker();
                            } catch (IOException e) {

                            }
                            logger.warn("Error starting job tracker", t);
                            break;
                        }
                        
                        break;
                    }
                }
            }
        }, "JOB-TRACKER-INIT");

        return jobTrackerThread;
    }

    /**
     * The thread for the Task Tracker.
     */
    private static Thread getTaskTrackerThread() {
        Thread taskTrackerThread = new Thread(new Runnable() {

            public void run() {
                taskTracker = null;

                while (true) {
                    try {
                        taskTracker = new TaskTracker(new CassandraJobConf());
                        taskTrackerMBean = MBeans.register("TaskTracker", "TaskTrackerInfo", taskTracker);
                        logger.info("Hadoop Task Tracker Started... ");
                        taskTracker.run();
                        logger.info("TaskTracker has finished");
                        break;
                    } catch (Throwable t) {
                        // Shutdown the Task Tracker
                        if (t instanceof InterruptedException) {
                            try {
                                taskTracker.shutdown();
                                logger.info("Task tracker was shutdown");
                            } catch (Exception e) {
                                logger.warn("Interruption when shutting down the task tracker");
                            }
                            break;
                        }

                        // on OOM shut down the tracker
                        if (t instanceof OutOfMemoryError || t.getCause() instanceof OutOfMemoryError) {
                            break;
                        }
                    }
                }
            }
        }, "TASK-TRACKER-INIT");

        return taskTrackerThread;
    }

}

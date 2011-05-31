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
package com.datastax.brisk;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.trackers.TrackerInitializer;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

public class BriskDaemon extends org.apache.cassandra.service.AbstractCassandraDaemon implements BriskDaemonMBean
{

    private static Logger logger = LoggerFactory.getLogger(BriskDaemon.class);
    private ThriftServer server;


    public String getReleaseVersion()
    {
        try
        {
            InputStream in = BriskDaemon.class.getClassLoader().getResourceAsStream("com/datastax/brisk/version.properties");
            if (in == null)
            {
                return "Unknown";
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("BriskVersion");
        }
        catch (Exception e)
        {
            logger.warn("Unable to load version.properties", e);
            return "debug version";
        }
    }
    
    protected void setup() throws IOException
    {     
        super.setup();
     
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("com.datastax.brisk:type=BriskDaemon"));
        }

        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        

    }

    protected void startServer()
    {
        if (server == null)
        {
            server = new ThriftServer(listenAddr, listenPort);
            server.start();
        }
        
        
        //Start hadoop trackers...
        if(TrackerInitializer.isTrackerNode)
        {
            logger.info("Starting up Hadoop trackers");
            TrackerInitializer.init();
        }

    }

    protected void stopServer()
    {
        if (server != null)
        {
            server.stopServer();
            try
            {
                server.join();
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting thrift server to stop", e);
            }
            server = null;
        }
    }

    /**
     * Simple class to run the thrift connection accepting code in separate
     * thread of control.
     */
    private static class ThriftServer extends Thread
    {
        private TServer serverEngine;

        public ThriftServer(InetAddress listenAddr, int listenPort)
        {
            // now we start listening for clients
            final BriskServer briskServer = new BriskServer();
            Brisk.Processor processor = new Brisk.Processor(briskServer);

            // Transport
            TServerSocket tServerSocket = null;

            try
            {
                tServerSocket = new TCustomServerSocket(new InetSocketAddress(listenAddr, listenPort),
                        DatabaseDescriptor.getRpcKeepAlive(),
                        DatabaseDescriptor.getRpcSendBufferSize(),
                        DatabaseDescriptor.getRpcRecvBufferSize());
            }
            catch (TTransportException e)
            {
                throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s",
                            listenAddr, listenPort), e);
            }

            logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

            // Protocol factory
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true,
                    true,
                    DatabaseDescriptor.getThriftMaxMessageLength());

            // Transport factory
            TTransportFactory inTransportFactory, outTransportFactory;

            int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
            inTransportFactory  = new TFramedTransport.Factory(tFramedTransportSize);
            outTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            logger.info("Using TFastFramedTransport with a max frame size of {} bytes.", tFramedTransportSize);

            TThreadPoolServer.Args args = new TThreadPoolServer.Args(tServerSocket)
            .minWorkerThreads(DatabaseDescriptor.getRpcMinThreads())
            .maxWorkerThreads(DatabaseDescriptor.getRpcMaxThreads())
            .inputTransportFactory(inTransportFactory)
            .outputTransportFactory(outTransportFactory)
            .inputProtocolFactory(tProtocolFactory)
            .outputProtocolFactory(tProtocolFactory)
            .processor(processor);

            ExecutorService executorService = new CleaningThreadPool(briskServer.clientState,
                    args.minWorkerThreads,
                    args.maxWorkerThreads);
            serverEngine = new CustomTThreadPoolServer(args, executorService);
        }
    
   

        public void run()
        {
            logger.info("Listening for thrift clients...");
            serverEngine.serve();
        }

        public void stopServer()
        {
            logger.info("Stop listening to thrift clients");
            serverEngine.stop();
        }
    }
    
    public static void main(String[] args)
    {
        new BriskDaemon().activate();
    }
    
}

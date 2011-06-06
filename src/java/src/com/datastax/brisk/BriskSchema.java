package com.datastax.brisk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.hadoop.CassandraProxyClient.ConnectionStrategy;
import org.apache.cassandra.locator.BriskSimpleSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Brisk;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class BriskSchema {

    private static Logger logger = Logger.getLogger(BriskSchema.class);

    public static final String KEYSPACE_NAME = "brisk_system";

    public static final String JOB_TRACKER_CF = "jobtracker";

    private static Brisk.Iface client;

    public static void setClient(Brisk.Iface aClient) {
        client = aClient;
    }

    public static Brisk.Iface getClient() {
        return client;
    }

    /**
     * Checks if the system keyspace exists.
     * 
     * @return The Keyspace definition or null if not found.
     * @throws IOException
     */
    public static KsDef checkKeyspace() throws IOException {
        try {
            return client.describe_keyspace(KEYSPACE_NAME);
        } catch (NotFoundException e) {
            return null;
        } catch (InvalidRequestException e) {
            throw new IOException(e);
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    public static KsDef createKeySpace() throws IOException {
        try {
            logger.info("Creating keyspace: " + KEYSPACE_NAME);
            Thread.sleep(new Random().nextInt(5000));

            KsDef cfsKs = checkKeyspace();

            if (cfsKs != null) {
                logger.info("keyspace already exists. Skipping creation.");
                return cfsKs;
            }

            List<CfDef> cfs = new ArrayList<CfDef>();

            CfDef cf = new CfDef();
            cf.setName(JOB_TRACKER_CF);
            cf.setComparator_type("BytesType");
            // there is only one row and one column.
            cf.setKey_cache_size(10);
            cf.setRow_cache_size(10);
            cf.setGc_grace_seconds(60);
            cf.setComment("Stores the current JobTracker node");
            cf.setKeyspace(KEYSPACE_NAME);

            cfs.add(cf);

            Map<String, String> stratOpts = new HashMap<String, String>();
            stratOpts.put(BriskSimpleSnitch.BRISK_DC, System.getProperty("cfs.replication", "1"));
            stratOpts.put(BriskSimpleSnitch.CASSANDRA_DC, "0");

            cfsKs = new KsDef().setName(KEYSPACE_NAME)
                    .setStrategy_class("org.apache.cassandra.locator.NetworkTopologyStrategy")
                    .setStrategy_options(stratOpts).setCf_defs(cfs);

            client.system_add_keyspace(cfsKs);
            waitForSchemaAgreement(client);

            return cfsKs;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static void waitForSchemaAgreement(Brisk.Iface aClient) throws InvalidRequestException,
            InterruptedException, TException {
        int waited = 0;
        int versions = 0;
        while (versions != 1) {
            ArrayList<String> liveschemas = new ArrayList<String>();
            Map<String, List<String>> schema = aClient.describe_schema_versions();
            for (Map.Entry<String, List<String>> entry : schema.entrySet()) {
                if (!entry.getKey().equals("UNREACHABLE"))
                    liveschemas.add(entry.getKey());
            }
            versions = liveschemas.size();
            Thread.sleep(1000);
            waited += 1000;
            if (waited > StorageService.RING_DELAY)
                throw new RuntimeException("Could not reach schema agreement in " + StorageService.RING_DELAY + "ms");
        }
    }

    public static void init() throws IOException {
        if (client != null) {
            return;
        }

        String host;
        if (DatabaseDescriptor.getRpcAddress() != null)
            host = DatabaseDescriptor.getRpcAddress().getHostName();
        else
            host = FBUtilities.getLocalAddress().getHostName();
        int port = DatabaseDescriptor.getRpcPort(); // default

        while (true) {
            try {
                client = CassandraProxyClient.newProxyConnection(host, port, true, ConnectionStrategy.STICKY);
                break;
            } catch (IOException e) {
                logger.info("Thrift RPC not ready. Sleeping 5 seconds...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    logger.warn("Thread interrupted when waiting for RPC to start. Aborting....");
                    break;
                }
            }
        }
    }

}

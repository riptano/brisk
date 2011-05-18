package org.apache.cassandra.hadoop.hive.metastore;

import java.io.IOException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Ignore;


public abstract class MetaStoreTestBase extends CleanupHelper
{

    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException
    {
        EmbeddedServer.startBrisk();                
    }
    
    protected Configuration buildConfiguration() 
    {
        Configuration conf = new Configuration();
        conf.set(CassandraClientHolder.CONF_PARAM_HOST, "localhost");
        conf.setInt(CassandraClientHolder.CONF_PARAM_PORT, DatabaseDescriptor.getRpcPort());
        conf.setBoolean(CassandraClientHolder.CONF_PARAM_FRAMED, true);
        conf.set(CassandraClientHolder.CONF_PARAM_CONNECTION_STRATEGY, "STICKY");
        return conf;
    }
}

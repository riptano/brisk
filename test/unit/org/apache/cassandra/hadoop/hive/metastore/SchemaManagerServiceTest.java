package org.apache.cassandra.hadoop.hive.metastore;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class SchemaManagerServiceTest extends MetaStoreTestBase
{
    private CassandraClientHolder cassandraClientHolder;
    private Configuration configuration;
    private SchemaManagerService schemaManagerService;
    private CassandraHiveMetaStore cassandraHiveMetaStore;
    
    public void setupLocal() throws Exception 
    {
        configuration = buildConfiguration();
        cassandraClientHolder = new CassandraClientHolder(configuration);
        cassandraHiveMetaStore = new CassandraHiveMetaStore();        
        schemaManagerService = new SchemaManagerService(cassandraHiveMetaStore, configuration);
    }
    
    @Test
    public void testMetaStoreSchema() throws Exception 
    {
        setupLocal();
        boolean created = schemaManagerService.createMetaStoreIfNeeded();
        assertTrue(created);
        created = schemaManagerService.createMetaStoreIfNeeded();
        assertFalse(created);
    }
    
    @Test
    public void testDiscoverUnmappedKeyspaces() throws Exception 
    {
        setupLocal();
        setupOtherKeyspace("OtherKeyspace");
        // init the meta store for usage
        cassandraHiveMetaStore.setConf(configuration);
        List<KsDef> keyspaces = schemaManagerService.findUnmappedKeyspaces();
        boolean foundCreated = false;
        // don't impose a keyspace maintenance burden. Looking for specifics is good enough
        for (KsDef ksDef : keyspaces)
        {
            if ( StringUtils.equals(ksDef.name, "OtherKeyspace") )
            {
                foundCreated = true;
                break;
            }
        }
        assertTrue(foundCreated);
    }
    
    private void setupOtherKeyspace(String ksName) throws Exception
    {
        CfDef cf = new CfDef(ksName, 
                "OtherCf1");
        cf.setKey_validation_class("UTF8Type");
        cf.setComparator_type("UTF8Type");
        KsDef ks = new KsDef(ksName, 
                "org.apache.cassandra.locator.SimpleStrategy",  
                Arrays.asList(cf));
        ks.setStrategy_options(KSMetaData.optsWithRF(configuration.getInt(CassandraClientHolder.CONF_PARAM_REPLICATION_FACTOR, 1)));
        cassandraClientHolder.getClient().system_add_keyspace(ks);                  
    }

}

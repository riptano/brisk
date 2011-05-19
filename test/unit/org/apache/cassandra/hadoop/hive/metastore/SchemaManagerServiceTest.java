package org.apache.cassandra.hadoop.hive.metastore;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class SchemaManagerServiceTest extends MetaStoreTestBase
{
    private CassandraClientHolder cassandraClientHolder;
    private Configuration configuration;
    private SchemaManagerService schemaManagerService;
    private CassandraHiveMetaStore cassandraHiveMetaStore;
    
    @Before
    public void setupLocal() throws Exception 
    {
                
        configuration = buildConfiguration();
        if ( cassandraClientHolder == null )
            cassandraClientHolder = new CassandraClientHolder(configuration);
        if ( cassandraHiveMetaStore == null)
        {
            cassandraHiveMetaStore = new CassandraHiveMetaStore();
            cassandraHiveMetaStore.setConf(configuration);        
        }
        schemaManagerService = new SchemaManagerService(cassandraHiveMetaStore, configuration);             
                
    }
    
    @Test
    public void testMetaStoreSchema() throws Exception 
    {
        boolean created = schemaManagerService.createMetaStoreIfNeeded();
        assertFalse(created);
    }
    

    @Test
    public void testDiscoverUnmappedKeyspaces() throws Exception 
    {
        
        cassandraClientHolder.getClient().system_add_keyspace(setupOtherKeyspace("OtherKeyspace")); 
        // init the meta store for usage

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
    

    @Test
    public void testCreateKeyspaceSchema() throws Exception
    {

        KsDef ksDef = setupOtherKeyspace("CreatedKeyspace");
        cassandraClientHolder.getClient().system_add_keyspace(ksDef);
        schemaManagerService.createKeyspaceSchema(ksDef);
        List<KsDef> keyspaces = schemaManagerService.findUnmappedKeyspaces();
        
        // don't impose a keyspace maintenance burden. Looking for specifics is good enough
        for (KsDef ks : keyspaces)
        {
            if ( StringUtils.equals(ks.name, "CreatedKeyspace") )
            {
                fail("created was not synched");         
            }
        }        
    }
    
    @Test
    public void testSkipCreateOnConfig() throws Exception
    {
        KsDef ksDef = setupOtherKeyspace("SkipCreatedKeyspace");
        cassandraClientHolder.getClient().system_add_keyspace(ksDef);               
        
        schemaManagerService.createKeyspaceSchemasIfNeeded();
        List<KsDef> keyspaces = schemaManagerService.findUnmappedKeyspaces();
        boolean skipped = false;
        for (KsDef ks : keyspaces)
        {
            if ( StringUtils.equals(ks.name, "SkipCreatedKeyspace") )
            {
                skipped = true;
            }
        }    
        assertTrue(skipped);
    }
    
    @Test
    public void testCreateOnConfig() throws Exception
    {
        KsDef ksDef = setupOtherKeyspace("ConfigCreatedKeyspace");
        cassandraClientHolder.getClient().system_add_keyspace(ksDef);
        configuration.setBoolean("cassandra.autoCreateSchema", true);        
        
        schemaManagerService.createKeyspaceSchemasIfNeeded();
        List<KsDef> keyspaces = schemaManagerService.findUnmappedKeyspaces();
        for (KsDef ks : keyspaces)
        {
            if ( StringUtils.equals(ks.name, "ConfigCreatedKeyspace") )
            {
                fail("keyspace not created by configuration");
            }
        }            
    }
   
    /**
     * Builds out a KsDef, does not persist.
     * @param ksName
     * @return
     * @throws Exception
     */
    private KsDef setupOtherKeyspace(String ksName) throws Exception
    {
        CfDef cf = new CfDef(ksName, 
                "OtherCf1");
        cf.setKey_validation_class("UTF8Type");
        cf.setComparator_type("UTF8Type");
        KsDef ks = new KsDef(ksName, 
                "org.apache.cassandra.locator.SimpleStrategy",  
                Arrays.asList(cf));
        ks.setStrategy_options(KSMetaData.optsWithRF(configuration.getInt(CassandraClientHolder.CONF_PARAM_REPLICATION_FACTOR, 1)));
        return ks;                 
    }   

}

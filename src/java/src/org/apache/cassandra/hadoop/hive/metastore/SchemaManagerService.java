package org.apache.cassandra.hadoop.hive.metastore;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service encapsulating schema managment of the Brisk platform. This service deals 
 * with both the Hive meta store schema as well as maintaining the mappings of 
 * column family and keyspace objects to meta store tables and databases respectively.
 * 
 * @author zznate
 */
public class SchemaManagerService
{

    private static Logger log = LoggerFactory.getLogger(SchemaManagerService.class);
    private CassandraClientHolder cassandraClientHolder;
    private Configuration configuration;
    private CassandraHiveMetaStore cassandraHiveMetaStore;
    
    public SchemaManagerService(CassandraHiveMetaStore cassandraHiveMetaStore, Configuration configuration)
    {
        this.cassandraHiveMetaStore = cassandraHiveMetaStore;
        this.configuration = configuration;
        this.cassandraClientHolder = new CassandraClientHolder(this.configuration);
    }
    
    /**
     * Create the meta store keyspace if it does not already exist.
     * @return true if the keyspace did no exist and the creation was successful. False if the 
     * keyspace already existed. 
     * @throws {@link CassandraHiveMetaStoreException} wrapping the underlying exception if we
     * failed to create the keyspace.
     */
    public boolean createMetaStoreIfNeeded() 
    {
        // Database_entities : {[name].name=name}
        // FIXME add these params to configuration
        // databaseName=metastore_db;create=true
        try 
        {
            cassandraClientHolder.applyKeyspace();
            return false;
        }
        catch (CassandraHiveMetaStoreException chmse) 
        {
            log.debug("Attempting to create meta store keyspace: First set_keyspace call failed. Sleeping.");                       
        }
        
        //Sleep a random amount of time to stagger ks creations on many nodes       
        try
        {
            Thread.sleep(new Random().nextInt(5000));
        }
        catch (InterruptedException e1)
        {
          
        }
                                
        //check again...
        try 
        {
            cassandraClientHolder.applyKeyspace();
            return false;
        }  
        catch (CassandraHiveMetaStoreException chmse) 
        {
            log.debug("Attempting to create meta store keyspace after sleep.");
        }
        
        CfDef cf = new CfDef(cassandraClientHolder.getKeyspaceName(), 
                cassandraClientHolder.getColumnFamily());
        cf.setKey_validation_class("UTF8Type");
        cf.setComparator_type("UTF8Type");
        KsDef ks = new KsDef(cassandraClientHolder.getKeyspaceName(), 
                "org.apache.cassandra.locator.SimpleStrategy",  
                Arrays.asList(cf));
        ks.setStrategy_options(KSMetaData.optsWithRF(configuration.getInt(CassandraClientHolder.CONF_PARAM_REPLICATION_FACTOR, 1)));
        try 
        {
            cassandraClientHolder.getClient().system_add_keyspace(ks);
            return true;
        } 
        catch (Exception e) 
        {
            throw new CassandraHiveMetaStoreException("Could not create Hive MetaStore database: " + e.getMessage(), e);
        }    
    }
    
    /**
     * Returns a List of Keyspace definitions that are not yet created as 'databases' 
     * in the Hive meta store. The list of keyspaces required for brisk operation are ignored.
     * @return
     */
    public List<KsDef> findUnmappedKeyspaces() 
    {
        List<KsDef> defs;
        try 
        {
            defs = cassandraClientHolder.getClient().describe_keyspaces();
            
            for (Iterator<KsDef> iterator = defs.iterator(); iterator.hasNext();)
            {
                KsDef ksDef = iterator.next();
                String name = ksDef.name;
                log.debug("Found ksDef name: {}",name);
                if ( StringUtils.indexOfAny(name, SYSTEM_KEYSPACES) > -1 || isKeyspaceMapped(name))
                {
                    log.debug("REMOVING ksDef name from unmapped List: {}",name);
                    iterator.remove();
                }
            }
        }
        catch (Exception ex)
        {
            throw new CassandraHiveMetaStoreException("Could not retrieve unmapped keyspaces",ex);            
        }
        return defs;
    }
    
    /**
     * Returns true if this keyspaceName returns a Database via
     * {@link CassandraHiveMetaStore#getDatabase(String)}
     * @param keyspaceName
     * @return
     */
    public boolean isKeyspaceMapped(String keyspaceName) 
    {
        try 
        {
            return cassandraHiveMetaStore.getDatabase(keyspaceName) != null;            
        } 
        catch (NoSuchObjectException e) 
        {
            return false;    
        }        
    }
    
    public void createKeyspaceSchema(KsDef ksDef)
    {
        // table.setParameters: "EXTERNAL"="TRUE"
    }
    
    
    /**
     * Contains 'system', as well as keyspace names for meta store, and Cassandra File System
     * FIXME: need to ref. the configuration value of the meta store keyspace. Should also coincide
     * with BRISK-190 
     */
    public static final String[] SYSTEM_KEYSPACES = new String[] {
        "system", CassandraClientHolder.DEF_META_STORE_KEYSPACE, "cfs"
    };
    
}

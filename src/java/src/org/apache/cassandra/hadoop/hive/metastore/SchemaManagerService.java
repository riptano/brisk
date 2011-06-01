package org.apache.cassandra.hadoop.hive.metastore;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
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
    private Warehouse warehouse;
    
    public SchemaManagerService(CassandraHiveMetaStore cassandraHiveMetaStore, Configuration conf)
    {
        this.cassandraHiveMetaStore = cassandraHiveMetaStore;
        this.configuration = conf;
        this.cassandraClientHolder = new CassandraClientHolder(configuration);
        try {
            this.warehouse = new Warehouse(configuration);
        } catch (MetaException me) {
            throw new CassandraHiveMetaStoreException("Could not start schemaManagerService.", me);
        }
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
        if ( configuration.getBoolean("cassandra.skipMetaStoreCreate", false) )
            return false;
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
            Thread.sleep(5000);
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
    
    public KsDef getKeyspaceForDatabaseName(String databaseName)
    {        
        try 
        {
            return cassandraClientHolder.getClient().describe_keyspace(databaseName);
        } 
        catch (NotFoundException e) 
        {
            return null;
        }
        catch (Exception ex)
        {
            throw new CassandraHiveMetaStoreException("Problem finding Keyspace for databaseName " + databaseName, ex);
        }        
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
    
    /**
     * Creates the database based on the Keyspace's name. The tables
     * are created similarly based off the names of the column families.
     * Column family meta data will be used to define the table's fields.
     *  
     * @param ksDef
     */
    public void createKeyspaceSchema(KsDef ksDef)
    {
        try 
        {
            cassandraHiveMetaStore.createDatabase(buildDatabase(ksDef));
            for (CfDef cfDef : ksDef.cf_defs)
            {
                cassandraHiveMetaStore.createTable(buildTable(cfDef));
            }

        } 
        catch (InvalidObjectException ioe) 
        {
            throw new CassandraHiveMetaStoreException("Could not create keyspace schema.", ioe);
        }
        catch (MetaException me)
        {
            throw new CassandraHiveMetaStoreException("Problem persisting metadata", me);
        }

    }
    
    public void createKeyspaceSchemasIfNeeded()
    {
        if ( getAutoCreateSchema() )
        {
            List<KsDef> keyspaces = findUnmappedKeyspaces();
            for (KsDef ksDef : keyspaces)
            {
                createKeyspaceSchema(ksDef);
            }
        }
    }
    
    /**
     * Compares the column families in the keyspace with what we have in hive so far,
     * creating tables for any that do not exist as such already.
     * @param ksDef
     */
    public void createNewColumnFamilyTables(KsDef ksDef)
    {
        for (CfDef cfDef : ksDef.cf_defs)
        {
            try 
            {
                if ( cassandraHiveMetaStore.getTable(cfDef.keyspace, cfDef.name) == null)
                    cassandraHiveMetaStore.createTable(buildTable(cfDef));
            }
            catch (InvalidObjectException ioe) 
            {            
                throw new CassandraHiveMetaStoreException("Could not create table for CF: " + cfDef.name, ioe);
            }
            catch (MetaException me)
            {
                throw new CassandraHiveMetaStoreException("Problem persisting metadata for CF: " + cfDef.name, me);
            }
        }
    }
    
    /**
     * Check to see if we are configured to auto create schema
     * @return the value of 'cassandra.autoCreateHiveSchema' according to 
     * the configuration. False by default.
     */
    public boolean getAutoCreateSchema()
    {
        return configuration.getBoolean("cassandra.autoCreateHiveSchema", false); 
    }
    
    private Database buildDatabase(KsDef ksDef)
    {
        Database database = new Database();
        try 
        {
            database.setLocationUri(warehouse.getDefaultDatabasePath(ksDef.name).toString());
        } 
        catch (MetaException me) 
        {
            throw new CassandraHiveMetaStoreException("Could not determine storage URI of database", me);
        }
        database.setName(ksDef.name);
        return database;
    }
    
    private Table buildTable(CfDef cfDef)
    {
        Table table = new Table();
        table.setDbName(cfDef.keyspace);
        table.setTableName(cfDef.name);
        table.setTableType(TableType.EXTERNAL_TABLE.toString());
        table.putToParameters("EXTERNAL", "TRUE");
        table.putToParameters("cassandra.ks.name", cfDef.keyspace);
        table.putToParameters("cassandra.cf.name", cfDef.name);
        table.putToParameters("storage_handler", "org.apache.hadoop.hive.cassandra.CassandraStorageHandler");
        table.setPartitionKeys(new ArrayList<FieldSchema>());

        
        StorageDescriptor sd = new StorageDescriptor();
        sd.setInputFormat("org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardColumnInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.cassandra.output.HiveCassandraOutputFormat");
        sd.setParameters(new HashMap<String, String>());
        try {
            sd.setLocation(warehouse.getDefaultTablePath(cfDef.keyspace, cfDef.name).toString());
        } catch (MetaException me) {
            log.error("could not build path information correctly",me);
        }
        SerDeInfo serde = new SerDeInfo();
        serde.setSerializationLib("org.apache.hadoop.hive.cassandra.serde.StandardColumnSerDe");
        serde.putToParameters("serialization.format", "1");
        sd.setSerdeInfo(serde);
        
        if ( cfDef.getColumn_metadataSize() > 0 )
        {
            for (ColumnDef column : cfDef.getColumn_metadata() )
            {
                applyType(sd, column);
            }
        }        
        else
        {   
            // satisfy requirement of at least one col on a table
            sd.addToCols(new FieldSchema("defaultcol", "string", "Auto-created default column."));
        }
        table.setSd(sd);
        return table;
    }

    /**
     * Deduce the type information based on column validator, adding a FieldSchema to the provided
     * StorageDescriptor
     * @param sd
     * @param column
     */
    private void applyType(StorageDescriptor sd, ColumnDef column)
    {
        if ( log.isDebugEnabled() )
        {
            log.debug("Applying type information for column: {}", column.toString());
        }
        try 
        {
            // assume its a FQ classname if we find a period. Built-in otherwise.
            AbstractType<?> type = DatabaseDescriptor.getComparator(column.getValidation_class());
            
            switch (type.getJdbcType())
            {
            // UTF8Type
            case Types.VARCHAR:
                sd.addToCols(new FieldSchema(ByteBufferUtil.string(column.name), "string", buildTypeComment(type)));                        
                break;
            case Types.BINARY:
                // TODO not sure there is much we can do here outside of conversion to HEX
                sd.addToCols(new FieldSchema(ByteBufferUtil.bytesToHex(column.name), "string", buildTypeComment(type)));
                break;
                // IntegerType
            case Types.BIGINT:
                sd.addToCols(new FieldSchema(ByteBufferUtil.string(column.name), "bigint", buildTypeComment(type)));
                break;                
                // LongType
            case Types.INTEGER:
                sd.addToCols(new FieldSchema(ByteBufferUtil.string(column.name), "int", buildTypeComment(type)));
                break;
                // UUIDType variants are all 'other'
            default:
                // TODO same as binary, we'll just do a hex string for now
                sd.addToCols(new FieldSchema(ByteBufferUtil.bytesToHex(column.name), "string", buildTypeComment(type)));
                break;
            }
        }
        catch (Exception e) 
        {
            throw new CassandraHiveMetaStoreException("There was a problem determining type information while creating schemas",e);
        }
    }
    
    private static final String buildTypeComment(AbstractType type)
    {
        return String.format("Auto-created based on %s from Column Family meta data", type.getClass().getName());
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

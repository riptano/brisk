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
package org.apache.cassandra.hadoop.fs;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.hadoop.fs.StatusHolder.StatusHolderException;
import org.apache.cassandra.hadoop.fs.connection.ConnectionFactory;
import org.apache.cassandra.hadoop.fs.connection.ConnectionPoolableObjectFactory;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.BriskSimpleSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Brisk;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.LocalBlock;
import org.apache.cassandra.thrift.LocalOrRemoteBlock;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.StorageType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.datastax.brisk.concurrent.JMXEnabledThreadPoolExecutor;
import com.datastax.brisk.concurrent.NamedThreadFactory;

/**
 * 
 * CFs schema:
 * 
 * Column Families:
 * - inode
 * - sblocks
 * 
 * -------------------
 * |      inode       |
 * -------------------
 *  {key : [<path>: <  > ], [<sentinel>: <   >], [ <datacol> : < all blocks with its subBlocks serialized>] }
 *  
 *  ------------------
 * |     sblocks      |
 *  ------------------
 *  { key(Block UUID): [<subBlockUUID> : <data>>], [<subBlockUUID> : <data>>], .......[<subBlockUUID> : <data>>] }
 */
public class CassandraFileSystemThriftStore implements CassandraFileSystemStore
{
    private final static Logger         logger        = Logger.getLogger(CassandraFileSystemThriftStore.class);

    private static final String         keySpace      = "cfs";
    
    // Cfs for normal use. They can be overridden if the archive mode is set.
    private static String         inodeDefaultCf       = "inode";
    private static String         sblockDefaultCf       = "sblocks";
    
    // Cfs for archive kind of storage
    private static final String         inodeArchiveCf       = "inode_archive";
    private static final String         sblockArchiveCf       = "sblocks_archive";

    private static final ByteBuffer     dataCol       = ByteBufferUtil.bytes("data");
    private static final ByteBuffer     pathCol       = ByteBufferUtil.bytes("path");
    private static final ByteBuffer     sentCol       = ByteBufferUtil.bytes("sentinel");
    
    private String         inodeCfInUse       = null;
    private String         sblockCfInUse       = null;

    // This values can be overridden if the archive mode is set.
    private ColumnPath     sblockPath     = null;
    private ColumnParent   sblockParent   = null;

    // This values can be overridden if the archive mode is set.
    private ColumnPath     inodePath     = null;
    private ColumnParent   inodeParent   = null;

    // This values can be overridden if the archive mode is set.
    private ColumnPath     inodeDataPath = null;
    private ColumnPath     sblockDataPath = null;
    
    private StorageType storageTypeInUse  = StorageType.CFS_REGULAR;

    private static final SlicePredicate pathPredicate = new SlicePredicate().setColumn_names(Arrays.asList(pathCol));

    private static final ByteBuffer     sentinelValue = ByteBufferUtil.bytes("x");

    private ConsistencyLevel            consistencyLevelRead;

    private ConsistencyLevel            consistencyLevelWrite;

    private Cassandra.Iface             client;
    
    /**
     * Cassandra is faster at writing. This pool allows parallel writes.
     */
    private ExecutorService writersPool;
    
    private BlockingQueue<Runnable> writerQueue;
    
    private GenericObjectPool connectionPool;
    
    private ConnectionFactory connectionFactory;
    
    /**
     * Keep a Set per Thread to retrieve the status of a set of scheduled writes.
     */
    private static StatusHolder writersStatusHolder = new StatusHolder();

    public CassandraFileSystemThriftStore()
    {

    }

    public void initialize(URI uri, Configuration conf) throws IOException
    {
    	connectionFactory = new ConnectionFactory(uri, conf);
    	client = connectionFactory.createConnection();
    	
        KsDef ks = checkKeyspace();

        if (ks == null)
            ks = createKeySpace();

        initConsistencyLevels(ks, conf);
        initCFNames(uri);

        setKeyspaceToClient(client);
        
        int concurrentWriters = getSubBlockWriterThreads(conf);
        writerQueue = new LinkedBlockingQueue<Runnable>(getSubBlockWorkQueueSize(conf));
        writersPool = new JMXEnabledThreadPoolExecutor(concurrentWriters, 60, TimeUnit.SECONDS, writerQueue, 
        		new NamedThreadFactory("SUB_BLOCK-WRITERS-" + System.currentTimeMillis()));
        
        // Keep as max idle as min threads we have and block for 60 secs before throwing an exception.
        // Max is 2 times the amount of workers.
        connectionPool = new GenericObjectPool(new ConnectionPoolableObjectFactory(connectionFactory), concurrentWriters * 2,
        		GenericObjectPool.WHEN_EXHAUSTED_BLOCK, 60000L, concurrentWriters);
    }
 

    private void setKeyspaceToClient(Iface aClient) throws IOException {
        try
        {
        	aClient.set_keyspace(keySpace);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
	}

	private int getSubBlockWorkQueueSize(Configuration conf) {
		return conf.getInt("concurrent.writers.workqueue.size", Integer.MAX_VALUE);
	}

	private int getSubBlockWriterThreads(Configuration conf) {
		return conf.getInt("concurrent.writers.subblocks", Runtime.getRuntime().availableProcessors());
	}

	/**
     * Set to different set of Column Families is the archive location is selected.
     */
    private void initCFNames(URI uri) {
    	logger.info("Using storage:" + uri.getScheme());
    	
		if (isArchive(uri)) {
			// cfs-archive:///
			inodeCfInUse = inodeArchiveCf;
			sblockCfInUse = sblockArchiveCf;
			
			storageTypeInUse = StorageType.CFS_ARCHIVE;
		} else {
			// cfs:///
			inodeCfInUse = inodeDefaultCf;
			sblockCfInUse = sblockDefaultCf;
		}
		
		// Create the remaining paths and parents base on the CfInUse.
		
		sblockPath     = new ColumnPath(sblockCfInUse);
		sblockParent   = new ColumnParent(sblockCfInUse);
		
		inodePath     = new ColumnPath(inodeCfInUse);
		inodeParent   = new ColumnParent(inodeCfInUse);
		
		inodeDataPath = new ColumnPath(inodeCfInUse).setColumn(dataCol);
		sblockDataPath = new ColumnPath(sblockCfInUse).setColumn(dataCol);
		
	}

    /**
     * Returns TRUE is the <code>uri</code> correspond to an archive location.
     */
	private boolean isArchive(URI uri) {
		return uri.getScheme().startsWith("cfs-archive");
	}

	/**
     * Initialize the consistency levels for reads and writes.
     * 
     * @param ks
     *            Keyspace definition
     */
    private void initConsistencyLevels(KsDef ks, Configuration conf)
    {

        consistencyLevelRead = ConsistencyLevel.valueOf(conf.get("brisk.consistencylevel.read", "QUORUM"));
        consistencyLevelWrite = ConsistencyLevel.valueOf(conf.get("brisk.consistencylevel.write", "QUORUM"));

        // Change consistency if this using NTS
        if (ks.getStrategy_class().contains("NetworkTopologyStrategy"))
        {
            if (consistencyLevelRead.equals(ConsistencyLevel.QUORUM))
            {
                consistencyLevelRead = ConsistencyLevel.LOCAL_QUORUM;
            }
            if (consistencyLevelWrite.equals(ConsistencyLevel.QUORUM))
            {
                consistencyLevelWrite = ConsistencyLevel.LOCAL_QUORUM;
            }
        }
    }

    private KsDef checkKeyspace() throws IOException
    {
        try
        {
            return client.describe_keyspace(keySpace);
        }
        catch (NotFoundException e)
        {
            return null;
        }
        catch (InvalidRequestException e)
        {
            throw new IOException(e);
        }
        catch (TException e)
        {
            throw new IOException(e);
        }
    }

    public KsDef createKeySpace() throws IOException
    {
        try
        {
            // Stagger create time so nodes don't
            // get confused
            Thread.sleep(new Random().nextInt(5000));

            KsDef cfsKs = checkKeyspace();

            if (cfsKs != null)
                return cfsKs;

            List<CfDef> cfs = new ArrayList<CfDef>();

            CfDef cf = new CfDef();
            cf.setName(inodeDefaultCf);
            cf.setComparator_type("BytesType");
            cf.setKey_cache_size(1000000);
            cf.setRow_cache_size(0);
            cf.setGc_grace_seconds(60);
            cf.setComment("Stores file meta data");
            cf.setKeyspace(keySpace);

            // this is a workaround until 
            // http://issues.apache.org/jira/browse/CASSANDRA-1278
            cf.setMemtable_flush_after_mins(1);
            cf.setMemtable_throughput_in_mb(128);
            
            cf.setColumn_metadata(Arrays.asList(new ColumnDef(pathCol, "BytesType").setIndex_type(IndexType.KEYS)
                    .setIndex_name("path"), new ColumnDef(sentCol, "BytesType").setIndex_type(IndexType.KEYS)
                    .setIndex_name("sentinel")));

            cfs.add(cf);

            cf = new CfDef();
            cf.setName(sblockDefaultCf);
            cf.setComparator_type("BytesType");
            cf.setKey_cache_size(1000000);
            cf.setRow_cache_size(0);
            cf.setGc_grace_seconds(60);
            cf.setComment("Stores blocks of information associated with a inode");
            cf.setKeyspace(keySpace);

            // Optimization for 128 MB blocks.
            cf.setMemtable_throughput_in_mb(128);
            cf.setMemtable_flush_after_mins(1);

            cf.setMin_compaction_threshold(16);
            cf.setMax_compaction_threshold(64);
            
            cfs.add(cf);
            
            // CFs for archive
            cf = new CfDef();
            cf.setName(inodeArchiveCf);
            cf.setComparator_type("BytesType");
            cf.setKey_cache_size(1000000);
            cf.setRow_cache_size(0);
            cf.setGc_grace_seconds(60);
            cf.setComment("Stores file meta data");
            cf.setKeyspace(keySpace);

            // this is a workaround until 
            // http://issues.apache.org/jira/browse/CASSANDRA-1278
            cf.setMemtable_flush_after_mins(1);
            cf.setMemtable_throughput_in_mb(128);
            
            cf.setColumn_metadata(Arrays.asList(new ColumnDef(pathCol, "BytesType").setIndex_type(IndexType.KEYS)
                    .setIndex_name("path"), new ColumnDef(sentCol, "BytesType").setIndex_type(IndexType.KEYS)
                    .setIndex_name("sentinel")));

            cfs.add(cf);

            cf = new CfDef();
            cf.setName(sblockArchiveCf);
            cf.setComparator_type("BytesType");
            cf.setKey_cache_size(1000000);
            cf.setRow_cache_size(0);
            cf.setGc_grace_seconds(60);
            cf.setComment("Stores blocks of information associated with a inode");
            cf.setKeyspace(keySpace);

            // Optimization for 128 MB blocks.
            cf.setMemtable_flush_after_mins(1);
            cf.setMemtable_throughput_in_mb(128);
            // Disable compaction for archive.
            cf.setMin_compaction_threshold(0);
            cf.setMax_compaction_threshold(0);
            
            cfs.add(cf);
            
            Map<String,String> stratOpts = new HashMap<String,String>();
            stratOpts.put(BriskSimpleSnitch.BRISK_DC, System.getProperty("cfs.replication","1"));
            stratOpts.put(BriskSimpleSnitch.CASSANDRA_DC, "0");

            cfsKs = new KsDef()
                .setName(keySpace)
                .setStrategy_class("org.apache.cassandra.locator.NetworkTopologyStrategy")
                .setStrategy_options(stratOpts)
                .setDurable_writes(false)
                .setCf_defs(cfs);

            client.system_add_keyspace(cfsKs);
            waitForSchemaAgreement();

            return cfsKs;
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }

    }

    private void waitForSchemaAgreement() throws InvalidRequestException, InterruptedException, TException {
        int waited = 0;
        int versions = 0;
        while (versions != 1)
        {
            ArrayList<String> liveschemas = new ArrayList<String>();
            Map <String, List<String>> schema = client.describe_schema_versions();
            for (Map.Entry<String, List<String>> entry : schema.entrySet())
            {
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

    public InputStream retrieveBlock(Block block, long byteRangeStart) throws IOException
    {
    	return new CassandraSubBlockInputStream(this, block, byteRangeStart);
    }
    
    public InputStream retrieveSubBlock(Block block, SubBlock subBlock, long byteRangeStart) throws IOException
    {
    	ByteBuffer blockId = uuidToByteBuffer(block.id);
        ByteBuffer subBlockId = uuidToByteBuffer(subBlock.id);

        LocalOrRemoteBlock blockData = null;

        try
        {
            blockData = ((Brisk.Iface) client).get_cfs_sblock(FBUtilities.getLocalAddress().getHostName(), 
            		blockId, subBlockId, (int) byteRangeStart, storageTypeInUse);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }

        if (blockData == null)
            throw new IOException("Missing block: " + subBlock.id);

        InputStream is = null;
        if (blockData.remote_block != null)
            is = ByteBufferUtil.inputStream(blockData.remote_block);
        else
            is = readLocalBlock(blockData.getLocal_block());

        return is;
    }

	private InputStream readLocalBlock(LocalBlock blockInfo)
    {

        if (blockInfo.file == null)
            throw new RuntimeException("Local file name is not defined");

        if (blockInfo.length == 0)
            return ByteBufferUtil.inputStream(ByteBufferUtil.EMPTY_BYTE_BUFFER);

        RandomAccessFile raf = null;
        try
        {
            raf = new RandomAccessFile(blockInfo.file, "r");

            if(logger.isDebugEnabled())
                logger.debug("Mmapping " + blockInfo.length + " bytes");

            MappedByteBuffer bb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, blockInfo.offset,
                    blockInfo.length);

            return new ByteBufferUtil().inputStream(bb);

        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException("Local file does not exist: " + blockInfo.file);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Unable to mmap block %s[%d,%d]", blockInfo.file,
                    blockInfo.length, blockInfo.offset), e);
        }
        finally
        {
            FileUtils.closeQuietly(raf);
        }

    }

    public INode retrieveINode(Path path) throws IOException
    {
        ByteBuffer pathKey = getPathKey(path);
        ColumnOrSuperColumn pathInfo;

        pathInfo = performGet(pathKey, inodeDataPath, consistencyLevelRead);
        
        // If not found and I already tried with CL= ONE, retry with higher CL.
        if (pathInfo == null && consistencyLevelRead.equals(ConsistencyLevel.ONE))
        {
        	pathInfo = performGet(pathKey, inodeDataPath, ConsistencyLevel.QUORUM);          
        }

        if (pathInfo == null)
        {
            // Now give up and return null.
            return null;
        }
        
        return INode.deserialize(ByteBufferUtil.inputStream(pathInfo.column.value), pathInfo.column.getTimestamp());
    }

	private ColumnOrSuperColumn performGet(ByteBuffer key, ColumnPath cp, ConsistencyLevel cl) throws IOException {
        ColumnOrSuperColumn result;
        try 
        {
            result = client.get(key, cp, cl);
        }
        catch (NotFoundException e)
        {
            return null;
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
        
        return result;
	}

	/**
     * {@inheritDoc}
     */
    public void storeSubBlock(UUID parentBlockUUID, SubBlock sblock, ByteArrayOutputStream os) throws IOException
    {
    	assert parentBlockUUID != null;
    	
    	// Row key is the Block id to which this SubBLock belongs to.
        ByteBuffer parentBlockId = uuidToByteBuffer(parentBlockUUID);

        ByteBuffer data = ByteBuffer.wrap(os.toByteArray());
        
        if (logger.isDebugEnabled()) {
        	logger.debug("Storing " + sblock);
        }
        
        // Row Key: UUID of SubBLock Block parent
        // Column name: Sub Block UUID
        // Column value: Sub Block Data.

        try
        {
            client.insert(
                parentBlockId, 
                sblockParent, 
                new Column().setName(uuidToByteBuffer(sblock.id)).setValue(data).setTimestamp(System.currentTimeMillis()), 
                consistencyLevelWrite);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }
    
    
    public void scheduleStoreSubBlock(UUID parentBlockUUID, final SubBlock sblock, ByteArrayOutputStream os)
    {
    	assert parentBlockUUID != null;
    	
    	// Row key is the Block id to which this SubBLock belongs to.
        final ByteBuffer parentBlockId = uuidToByteBuffer(parentBlockUUID);

        // Clone the array to free it up since an independent
        final ByteBuffer data = ByteBuffer.wrap(os.toByteArray().clone());
        
        Future<Boolean> subBlockWriterFutureStatus = writersPool.submit(new Callable<Boolean>() {

			private boolean thereWasError = false;

			// Perform the write in a separate thread.
			@Override
			public Boolean call() throws Exception {
				
		        if (logger.isDebugEnabled()) {
		        	logger.debug("Storing " + sblock);
		        }
				
				Cassandra.Iface innerClient = null;
				
	            try
	            {
	            	// Borrow a client
	            	innerClient = (Cassandra.Iface) connectionPool.borrowObject();
	            	setKeyspaceToClient(innerClient);
	            	
	                // Row Key: UUID of SubBLock Block parent
	                // Column name: Sub Block UUID
	                // Column value: Sub Block Data.
	            	
	            	innerClient.insert(
	                    parentBlockId, 
	                    sblockParent, 
	                    new Column().setName(uuidToByteBuffer(sblock.id)).setValue(data).setTimestamp(System.currentTimeMillis()), 
	                    consistencyLevelWrite);
	            }
	            catch (Exception e)
	            {
	                //throw new IOException(e);
	            	logger.error("Unable to write subBlock", e);
	            	thereWasError = true;
	            }
	            finally 
	            {
	            	if (innerClient != null)
	            	{
	            		// Return a client to the pool.
	            		connectionPool.returnObject(innerClient);
	            	}
	            }
	            
	            return thereWasError;
			}
		});
        
        // Add the status to the status list of the current thread
        writersStatusHolder.addStatus(subBlockWriterFutureStatus);
        
    }
    
    public void waitForStoreSubBlockToComplete() throws IOException {
    	try {
			writersStatusHolder.waitForCompletion();
		} catch (StatusHolderException e) {
			throw new IOException(e);
		}	
    }

    public void storeINode(Path path, INode inode) throws IOException
    {
        if (logger.isDebugEnabled() && inode.getBlocks() != null) {
            logger.debug("Writing inode to: " + path);
        	printBlocksDebug(inode.getBlocks());
        }

        // Inode row key
        ByteBuffer pathKey = getPathKey(path);

        ByteBuffer data = inode.serialize();

        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        Map<String, List<Mutation>> pathMutations = new HashMap<String, List<Mutation>>();
        List<Mutation> mutations = new ArrayList<Mutation>();

        // setup mutation map
        pathMutations.put(inodeCfInUse, mutations);
        mutationMap.put(pathKey, pathMutations);

        long ts = System.currentTimeMillis();

        // file name
        mutations.add(new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(new Column().setName(
                pathCol).setValue(ByteBufferUtil.bytes(path.toUri().getPath())).setTimestamp(ts))));

        // sentinal
        mutations.add(new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(new Column().setName(
                sentCol).setValue(sentinelValue).setTimestamp(ts))));

        // serialized inode
        mutations.add(new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(new Column().setName(
                dataCol).setValue(data).setTimestamp(ts))));

        try
        {
            client.batch_mutate(mutationMap, consistencyLevelWrite);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }
    
    /**
     * Print this List by invoking its objects' toString(); using the logger in debug mode.
     * @param blocks list of blocks to be printed
     */
    private void printBlocksDebug(Block[] blocks) {
        for (Block block : blocks) {
            logger.debug(block);
        }
    }

    ByteBuffer getPathKey(Path path)
    {
        return ByteBufferUtil.bytes(FBUtilities.hashToBigInteger(ByteBufferUtil.bytes(path.toUri().getPath()))
                .toString(16));
    }


    ByteBuffer uuidToByteBuffer(UUID id)
    {
        return ByteBufferUtil.bytes(FBUtilities.bytesToHex(UUIDGen.decompose(id)));
    }

    /**
     * {@inheritDoc}
     */
    public void deleteSubBlocks(INode inode) throws IOException
    {
        // Get all the SubBlock keys to delete.
        List<UUID> subBlockKeys = getListOfBlockIds(inode.getBlocks());
        try
        {
            // TODO (patricioe) can we send one big batch mutation  here ?
            for (UUID subBlocksKey : subBlockKeys) {
                client.remove(ByteBuffer.wrap(UUIDGen.decompose(subBlocksKey)), sblockPath, System.currentTimeMillis(),
                        consistencyLevelWrite);
            }
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }
    
    /**
     * Retrieves a list of UUIDs
     * @param blocks list of blocks
     * @return a list of UUID
     */
    private List<UUID> getListOfBlockIds(Block[] blocks) {
		List<UUID> blockIds = new ArrayList<UUID>(blocks.length);
		for (Block aBlock : blocks) {
			blockIds.add(aBlock.id);
		}
		return blockIds;
	}

    /**
     * {@inheritDoc}
     */
    public void deleteINode(Path path) throws IOException
    {
        try
        {
            client.remove(getPathKey(path), inodePath, System.currentTimeMillis(), consistencyLevelWrite);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }

    public Set<Path> listDeepSubPaths(Path path) throws IOException
    {
        String startPath = path.toUri().getPath();

        List<IndexExpression> indexExpressions = new ArrayList<IndexExpression>();

        indexExpressions.add(new IndexExpression(sentCol, IndexOperator.EQ, sentinelValue));
        indexExpressions.add(new IndexExpression(pathCol, IndexOperator.GT, ByteBufferUtil.bytes(startPath)));

        // Limit listings to this root by incrementing the last char
        if (startPath.length() > 1)
        {
            String endPath = startPath.substring(0, startPath.length() - 1)
                    + new Character((char) (startPath.charAt(startPath.length() - 1) + 1));

            indexExpressions.add(new IndexExpression(pathCol, IndexOperator.LT, ByteBufferUtil.bytes(endPath)));
        }

        try
        {
            List<KeySlice> keys = client.get_indexed_slices(inodeParent, new IndexClause(indexExpressions,
                    ByteBufferUtil.EMPTY_BYTE_BUFFER, 100000), pathPredicate, consistencyLevelRead);

            Set<Path> matches = new HashSet<Path>(keys.size());

            for (KeySlice key : keys)
            {
                for (ColumnOrSuperColumn cosc : key.getColumns())
                {
                    matches.add(new Path(ByteBufferUtil.string(cosc.column.value)));
                }
            }

            return matches;
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }

    public Set<Path> listSubPaths(Path path) throws IOException
    {
        Set<Path> allPaths = listDeepSubPaths(path);
        Set<Path> prunedPath = new HashSet<Path>();

        for (Path p : allPaths)
        {
            if (p.depth() == (path.depth() + 1))
            {
                prunedPath.add(p);
            }
        }

        return prunedPath;
    }

    public String getVersion() throws IOException
    {
        return "Cassandra FileSystem Thrift Store";
    }

    public BlockLocation[] getBlockLocation(List<Block> blocks, long start, long len) throws IOException
    {
        if (blocks.isEmpty())
            return null;

        List<ByteBuffer> blockKeys = new ArrayList<ByteBuffer>(blocks.size());

        for (Block b : blocks)
            blockKeys.add(uuidToByteBuffer(b.id));

        BlockLocation[] locations = new BlockLocation[blocks.size()];

        try
        {
            List<List<String>> blockEndpoints = ((Brisk.Iface) client).describe_keys(keySpace, blockKeys);

            for (int i = 0; i < blockEndpoints.size(); i++)
            {
                List<String> endpoints = blockEndpoints.get(i);
                Block b = blocks.get(i);

                long offset = (i == 0 && b.offset > start) ? start : b.offset;

                // TODO: Add topology info if at all possible?
                locations[i] = new BlockLocation(null, endpoints.toArray(new String[0]), offset, b.length);
            }

            return locations;
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }

    }

	@Override
	public void close() throws IOException {
		try {
			writersPool.shutdown();
			writersPool.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("Some writers were killed after 30 secs. It can be a bug. Please report!", e);
		}
		
		try {
			connectionPool.close();
		} catch (Exception e) {
			logger.warn("Error while closing the connection pool");
			// It kinda sucks but I need to be complaint with the interface.
			throw new IOException(e);
		}
	}
}

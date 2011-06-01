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
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.hadoop.trackers.CassandraJobConf;
import org.apache.cassandra.hadoop.trackers.TrackerInitializer;
import org.apache.cassandra.hadoop.trackers.TrackerManager;
import org.apache.cassandra.hadoop.trackers.TrackerManagerException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableReader.Operator;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.CachingMessageProducer;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.DatacenterReadCallback;
import org.apache.cassandra.service.DigestMismatchException;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.service.IResponseResolver;
import org.apache.cassandra.service.RangeSliceResponseResolver;
import org.apache.cassandra.service.ReadCallback;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Filter;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import static com.datastax.brisk.BriskDBUtil.validateAndGetColumn;

public class BriskServer extends CassandraServer implements Brisk.Iface
{

    static final Logger     logger         = Logger.getLogger(BriskServer.class);

    static final ByteBuffer dataCol        = ByteBufferUtil.bytes("data");

    static final String     cfsKeyspace    = "cfs";

    // CFs for regular storage
    static final String     cfsInodeDefaultFamily = "inode";
    static final String     cfsSubBlockDefaultFamily = "sblocks";

    static final QueryPath    inodeDefaultQueryPath =  new QueryPath(cfsInodeDefaultFamily, null, dataCol);
    static final ColumnParent subBlockDefaultDataPath= new ColumnParent(cfsSubBlockDefaultFamily);

    // CFs for archive kind of storage
    private static final String         cfsInodeArchiveFamily       = "inode_archive";
    private static final String         cfsSubBlockArchiveFamily       = "sblocks_archive";

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();
    private final static List<Column> EMPTY_SUBCOLUMNS = Collections.emptyList();
    private final static List<CounterColumn> EMPTY_COUNTER_SUBCOLUMNS = Collections.emptyList();

    static final QueryPath    inodeArchiveQueryPath =  new QueryPath(cfsInodeArchiveFamily, null, dataCol);
    static final ColumnParent subBlockArchiveDataPath= new ColumnParent(cfsSubBlockArchiveFamily);


	@Override
	public LocalOrRemoteBlock get_cfs_sblock(String callerHostName, ByteBuffer blockId, ByteBuffer sblockId, int offset,
			StorageType storageType) throws InvalidRequestException, UnavailableException, TimedOutException, NotFoundException,
			TException {

		if (storageType == StorageType.CFS_REGULAR)
		{
			return get_cfs_sblock(callerHostName, cfsSubBlockDefaultFamily, blockId, sblockId, offset, subBlockDefaultDataPath);
		} else
		{
			return get_cfs_sblock(callerHostName, cfsSubBlockArchiveFamily, blockId, sblockId, offset, subBlockArchiveDataPath);
		}

	}

    private LocalOrRemoteBlock get_cfs_sblock(String callerHostName, String subBlockCFName, ByteBuffer blockId,
    		ByteBuffer sblockId, int offset, ColumnParent subBlockDataPath) throws TException, TimedOutException, UnavailableException, InvalidRequestException, NotFoundException
    {

        // This logic is only used on mmap spec machines
        if (DatabaseDescriptor.getDiskAccessMode() == DiskAccessMode.mmap)
        {
            if(logger.isDebugEnabled())
                logger.debug("Checking for local block: "+blockId+" from "+callerHostName+" on "+FBUtilities.getLocalAddress().getHostName() );

            List<String> hosts = getKeyLocations(blockId);
            boolean isLocal = false;

            for (String hostName : hosts)
            {
                if(logger.isDebugEnabled())
                    logger.debug("Block " + blockId + " lives on " + hostName);

                if (hostName.equals(callerHostName) && hostName.equals(FBUtilities.getLocalAddress().getHostName()))
                {
                    isLocal = true;

                    break;
                }
            }

            if(isLocal)
            {
                if(logger.isDebugEnabled())
                    logger.debug("Local block should be on this node "+blockId);

                LocalBlock localBlock = getLocalSubBlock(subBlockCFName, blockId, sblockId, offset);

                if(localBlock != null)
                {
                    if(logger.isDebugEnabled())
                        logger.debug("Local block found: "+localBlock);

                    return new LocalOrRemoteBlock().setLocal_block(localBlock);
                }
            }
        }

        if(logger.isDebugEnabled())
            logger.debug("Checking for remote block: "+blockId);

        //Fallback to storageProxy
        return getRemoteSubBlock(blockId, sblockId, offset, subBlockDataPath);

    }


	public List<List<String>> describe_keys(String keyspace, List<ByteBuffer> keys) throws TException
    {
        List<List<String>> keyEndpoints = new ArrayList<List<String>>(keys.size());

        for (ByteBuffer key : keys)
        {
            keyEndpoints.add(getKeyLocations(key));
        }

        return keyEndpoints;
    }

    private List<String> getKeyLocations(ByteBuffer key)
    {
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(cfsKeyspace, key);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

        List<String> hosts = new ArrayList<String>(endpoints.size());

        for (InetAddress endpoint : endpoints)
        {
            hosts.add(endpoint.getHostName());
        }

        return hosts;
    }

    /**
     * Retrieves a local subBlock
     *
     * @param blockId row key
     * @param sblockId SubBlock column name
     * @param offset inside the sblock
     * @return a local sublock
     * @throws TException
     */
    private LocalBlock getLocalSubBlock(String subBlockCFName, ByteBuffer blockId, ByteBuffer sblockId, int offset) throws TException
    {
        DecoratedKey<Token<?>> decoratedKey = new DecoratedKey<Token<?>>(StorageService.getPartitioner().getToken(blockId), blockId);

        Table table = Table.open(cfsKeyspace);
        ColumnFamilyStore sblockStore = table.getColumnFamilyStore(subBlockCFName);

        Collection<SSTableReader> sstables = sblockStore.getSSTables();

        for (SSTableReader sstable : sstables)
        {

            long position = sstable.getPosition(decoratedKey, Operator.EQ);

            if (position == -1)
                continue;

            String filename = sstable.descriptor.filenameFor(Component.DATA);
            RandomAccessFile raf = null;
            int mappedLength = -1;
            MappedByteBuffer mappedData = null;
            MappedFileDataInput file = null;
            try
            {
                raf = new RandomAccessFile(filename, "r");
                assert position < raf.length();

                mappedLength = (raf.length() - position) < Integer.MAX_VALUE ? (int) (raf.length() - position)
                        : Integer.MAX_VALUE;

                mappedData = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, position, mappedLength);

                file = new MappedFileDataInput(mappedData, filename, 0);

                if (file == null)
                    continue;

                //Verify key was found in data file
                DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                        sstable.descriptor,
                        ByteBufferUtil.readWithShortLength(file));
                assert keyInDisk.equals(decoratedKey) : String.format("%s != %s in %s", keyInDisk, decoratedKey, file.getPath());

                long rowSize = SSTableReader.readRowSize(file, sstable.descriptor);

                assert rowSize > 0;
                assert rowSize < mappedLength;

                Filter bf = IndexHelper.defreezeBloomFilter(file, sstable.descriptor.usesOldBloomFilter);

                //verify this column in in this version of the row.
                if(!bf.isPresent(sblockId))
                    continue;

                List<IndexHelper.IndexInfo> indexList = IndexHelper.deserializeIndex(file);

                // we can stop early if bloom filter says none of the
                // columns actually exist -- but,
                // we can't stop before initializing the cf above, in
                // case there's a relevant tombstone
                ColumnFamilySerializer serializer = ColumnFamily.serializer();
                try
                {
                    ColumnFamily cf = serializer.deserializeFromSSTableNoColumns(ColumnFamily.create(sstable.metadata),
                            file);

                    if (cf.isMarkedForDelete())
                        continue;

                }
                catch (Exception e)
                {
                    e.printStackTrace();

                    throw new IOException(serializer + " failed to deserialize " + sstable.getColumnFamilyName()
                            + " with " + sstable.metadata + " from " + file, e);
                }


                Integer sblockLength = null;

                if(indexList == null)
                    sblockLength = seekToSubColumn(sstable.metadata, file, sblockId);
                else
                    sblockLength = seekToSubColumn(sstable.metadata, file, sblockId, indexList);


                if(sblockLength == null || sblockLength < 0)
                    continue;


                int bytesReadFromStart = mappedLength - (int)file.bytesRemaining();

                if(logger.isDebugEnabled())
                    logger.debug("BlockLength = "+sblockLength+" Availible "+file.bytesRemaining());

                assert offset <= sblockLength : String.format("%d > %d", offset,  sblockLength);

                long dataOffset = position + bytesReadFromStart;

                if(file.bytesRemaining() == 0 || sblockLength == 0)
                    continue;


                return new LocalBlock(file.getPath(), dataOffset + offset, sblockLength - offset);

            }
            catch (IOException e)
            {
                throw new TException(e);
            }
            finally
            {
                FileUtils.closeQuietly(raf);
            }
        }


        return null;
    }

    //Called when there are is no row index (meaning small number of columns)
    private Integer seekToSubColumn(CFMetaData metadata, FileDataInput file, ByteBuffer sblockId) throws IOException
    {
        int columns = file.readInt();
        for (int i = 0; i < columns; i++)
        {
            Integer dataLength = isSubBlockFound(metadata, file, sblockId);


            if(dataLength == null)
                return null;

            if(dataLength < 0)
                continue;

            return dataLength;

        }

        return null;
    }

    /**
     * Checks if the current column is the one we are looking for
     * @param metadata
     * @param file
     * @param sblockId
     * @return if > 0 the length to read from current file offset. if -1 not relevent. if null out of bounds
     */
    private Integer isSubBlockFound(CFMetaData metadata, FileDataInput file, ByteBuffer sblockId) throws IOException
    {
        ByteBuffer name = ByteBufferUtil.readWithShortLength(file);

        //Stop if we've gone too far (return null)
        if(metadata.comparator.compare(name, sblockId) > 0)
            return null;

        // verify column type;
        int b = file.readUnsignedByte();

        // skip ts (since we know block ids are unique)
        long ts = file.readLong();
        int sblockLength = file.readInt();

        if(!name.equals(sblockId) || (b & ColumnSerializer.DELETION_MASK) != 0 || (b & ColumnSerializer.EXPIRATION_MASK) != 0)
        {
            FileUtils.skipBytesFully(file, sblockLength);
            return -1;
        }

        return sblockLength;
    }

    private Integer seekToSubColumn(CFMetaData metadata, FileDataInput file, ByteBuffer sblockId, List<IndexHelper.IndexInfo> indexList) throws IOException
    {
        file.readInt(); // column count

        /* get the various column ranges we have to read */
        AbstractType comparator = metadata.comparator;

        int index = IndexHelper.indexFor(sblockId, indexList, comparator, false);
        if (index == indexList.size())
            return null;

        IndexHelper.IndexInfo indexInfo = indexList.get(index);
        if (comparator.compare(sblockId, indexInfo.firstName) < 0)
            return null;

        FileMark mark = file.mark();

        FileUtils.skipBytesFully(file, indexInfo.offset);

        while (file.bytesPastMark(mark) < indexInfo.offset + indexInfo.width)
        {
            Integer dataLength = isSubBlockFound(metadata, file, sblockId);

            if(dataLength == null)
                return null;

            if(dataLength < 0)
                continue;

            return dataLength;
        }

        return null;
    }

    private LocalOrRemoteBlock getRemoteSubBlock(ByteBuffer blockId, ByteBuffer sblockId, int offset, ColumnParent subBlockDataPath)
    	throws TimedOutException, UnavailableException, InvalidRequestException, NotFoundException
    {
        // The column name is the SubBlock id (UUID)
        ReadCommand rc = new SliceByNamesReadCommand(cfsKeyspace, blockId, subBlockDataPath, Arrays.asList(sblockId));

        try
        {
            // CL=ONE as there are NOT multiple versions of the blocks.
            List<Row> rows = StorageProxy.read(Arrays.asList(rc), ConsistencyLevel.ONE);

            IColumn col = null;
            try
            {
            	col = validateAndGetColumn(rows, sblockId);
            } catch (NotFoundException e)
            {
            	// This is a best effort to get the value. Sometimes due to the size of
            	// the sublocks, the normal replication may time out leaving a replicate without
            	// the piece of data. Hence we re try with higher CL.
            	rows = StorageProxy.read(Arrays.asList(rc), ConsistencyLevel.QUORUM);
            }

            col = validateAndGetColumn(rows, sblockId);

            ByteBuffer value = col.value();

            if(value.remaining() < offset)
                throw new InvalidRequestException("Invalid offset for block of size: "+value.remaining());


            LocalOrRemoteBlock block = new LocalOrRemoteBlock();
            if(offset > 0)
            {
                ByteBuffer offsetBlock = value.duplicate();
                offsetBlock.position(offsetBlock.position()+offset);
                block.setRemote_block(offsetBlock);
            }
            else
            {
                block.setRemote_block(value);
            }

            return block;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
    }


    public String get_jobtracker_address() throws NotFoundException, TException
    {
        if(!TrackerInitializer.isTrackerNode)
        {
            throw new NotFoundException();
        }

        return CassandraJobConf.getJobTrackerNode().getHostName()+":8012";
    }

	@Override
	public String move_job_tracker(String newJobtracker) throws NotFoundException, TException {

		try {
			TrackerManager.insertJobtrackerLocation(InetAddress.getByName(newJobtracker));
		} catch (UnknownHostException e) {
			throw new TException("Unable to set the new Job Tracker lcoation");
		} catch (TrackerManagerException e) {
			throw new TException("Unable to set the new Job Tracker lcoation");
		}
		return "New JobTracker location: " + newJobtracker +
			" set successfully. Please wait and check Brisk web interface at: http://" + newJobtracker + ":50030";
	}

	/**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static List<AbstractBounds> getRestrictedRanges(IPartitioner p, final AbstractBounds queryRange, final KeyRange key_range)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.equals(StorageService.getPartitioner().getMinimumToken()))
        {
            if (logger.isDebugEnabled())
                logger.debug("restricted single token match for query " + queryRange);
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<Token> tokenInRing = tokenMetadata.sortedTokens();
        ArrayList<Token> tokenInRange = new ArrayList<Token>();

        TokenFactory tokenFac = p.getTokenFactory();
        Token left = tokenFac.fromString(key_range.start_token);
        Token right = tokenFac.fromString(key_range.end_token);
        Token firstToken = left;
        Token secondToken = right;
        if (left.compareTo(right) > 0) {
        	firstToken = right;
        	secondToken = left;
        }

        Token compareToken = firstToken;
        int compareTimes = 0;
        int max = tokenInRing.size();
        for (int i = 0; i < max; i++) {
        	Token thisToken = tokenInRing.get(i);
        	if (thisToken.compareTo(compareToken) == 0) {
        		tokenInRange.add(compareToken);
        		compareTimes++;
        		compareToken = secondToken;
        	} else if (thisToken.compareTo(compareToken) < 0) {
        		//move to the next
        	} else {
        		//First time hit the compareToken
        		tokenInRange.add(compareToken);
        		i--; //compare the next token;
        		compareTimes++;
        		compareToken = secondToken;
        	}

        	if (compareTimes == 2) {
        		break;
        	}

        }

        if (compareTimes == 1 && tokenInRing.get(max - 1).compareTo(secondToken) > 0) {
        	tokenInRange.add(secondToken);
        }

        if (logger.isDebugEnabled())
        	logger.debug("tokens in range are " + tokenInRange + " with key range between " + left + " and " + right);


        List<AbstractBounds> ranges = new ArrayList<AbstractBounds>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenInRange, queryRange.left, true);
        AbstractBounds remainder = queryRange;
        while (ringIter.hasNext())
        {
            Token token = ringIter.next();
            if (remainder == null || !(remainder.left.equals(token) || remainder.contains(token)))
                // no more splits
                break;
            Pair<AbstractBounds,AbstractBounds> splits = remainder.split(token);
            if (splits.left != null)
                ranges.add(splits.left);
            remainder = splits.right;
        }
        if (remainder != null)
            ranges.add(remainder);
        if (logger.isDebugEnabled())
            logger.debug("restricted ranges for query " + queryRange + " are " + ranges);

        return ranges;
    }

	@Override
	public List<KeySlice> get_indexed_slices(ColumnParent column_parent,
			IndexClause index_clause, final KeyRange key_range,
			SlicePredicate column_predicate, ConsistencyLevel consistency_level)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
		logger.debug("scan");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        final String keyspace = state().getKeyspace();
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, column_predicate);
        ThriftValidation.validateIndexClauses(metadata, index_clause);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level);

        List<Row> rows = new ArrayList<Row>(index_clause.count);
        try
        {
        	IPartitioner p = StorageService.getPartitioner();

            Token leftToken = index_clause.start_key == null ? p.getMinimumToken() : p.getToken(index_clause.start_key);

            List<AbstractBounds> ranges = getRestrictedRanges(p, new Bounds(leftToken, p.getMinimumToken()), key_range);
            logger.debug("scan ranges are " + StringUtils.join(ranges, ","));

            // now scan until we have enough results
            for (AbstractBounds range : ranges)
            {
                List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, range.right);
                DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), liveEndpoints);

                // collect replies and resolve according to consistency level
                RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(keyspace, liveEndpoints);
                IReadCommand iCommand = new IReadCommand()
                {
                    public String getKeyspace()
                    {
                        return keyspace;
                    }
                };
                ReadCallback<Iterable<Row>> handler = getReadCallback(resolver, iCommand, consistency_level, liveEndpoints);
                handler.assureSufficientLiveNodes();

                IndexScanCommand command = new IndexScanCommand(keyspace, column_parent.column_family, index_clause, column_predicate, range);
                MessageProducer producer = new CachingMessageProducer(command);
                for (InetAddress endpoint : liveEndpoints)
                {
                    MessagingService.instance().sendRR(producer, endpoint, handler);
                    if (logger.isDebugEnabled())
                        logger.debug("reading " + command + " from " + endpoint);
                }

                try
                {
                    for (Row row : handler.get())
                    {
                        rows.add(row);
                        logger.debug("read " + row);
                    }
                }
                catch (TimeoutException ex)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Index scan timeout: " + ex.toString());
                    throw ex;
                }
                catch (DigestMismatchException e)
                {
                    throw new RuntimeException(e);
                }

                if (rows.size() >= index_clause.count)
                    rows = rows.subList(0, index_clause.count);
            }

        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }

        return thriftifyKeySlices(rows, column_parent, column_predicate);
	}

	static <T> ReadCallback<T> getReadCallback(IResponseResolver<T> resolver, IReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> endpoints)
    {
        if (consistencyLevel.equals(ConsistencyLevel.LOCAL_QUORUM) || consistencyLevel.equals(ConsistencyLevel.EACH_QUORUM))
        {
            return new DatacenterReadCallback(resolver, consistencyLevel, command, endpoints);
        }
        return new ReadCallback(resolver, consistencyLevel, command, endpoints);
    }

	private List<KeySlice> thriftifyKeySlices(List<Row> rows, ColumnParent column_parent, SlicePredicate predicate)
    {
        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(row.cf, column_parent.super_column != null, reversed);
            keySlices.add(new KeySlice(row.key.key, thriftifiedColumns));
        }

        return keySlices;
    }

	private List<ColumnOrSuperColumn> thriftifyColumnFamily(ColumnFamily cf, boolean subcolumnsOnly, boolean reverseOrder)
    {
        if (cf == null || cf.getColumnsMap().size() == 0)
            return EMPTY_COLUMNS;
        if (subcolumnsOnly)
        {
            IColumn column = cf.getColumnsMap().values().iterator().next();
            Collection<IColumn> subcolumns = column.getSubColumns();
            if (subcolumns == null || subcolumns.isEmpty())
                return EMPTY_COLUMNS;
            else
                return thriftifyColumns(subcolumns, reverseOrder);
        }
        if (cf.isSuper())
        {
            boolean isCounterCF = cf.metadata().getDefaultValidator().isCommutative();
            return thriftifySuperColumns(cf.getSortedColumns(), reverseOrder, isCounterCF);
        }
        else
        {
            return thriftifyColumns(cf.getSortedColumns(), reverseOrder);
        }
    }

	private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<IColumn> columns, boolean reverseOrder, boolean isCounterCF)
    {
        if (isCounterCF)
            return thriftifyCounterSuperColumns(columns, reverseOrder);
        else
            return thriftifySuperColumns(columns, reverseOrder);
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<IColumn> columns, boolean reverseOrder)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column : columns)
        {
            List<Column> subcolumns = thriftifySubColumns(column.getSubColumns());
            if (subcolumns.isEmpty())
            {
                continue;
            }
            SuperColumn superColumn = new SuperColumn(column.name(), subcolumns);
            thriftSuperColumns.add(new ColumnOrSuperColumn().setSuper_column(superColumn));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private List<ColumnOrSuperColumn> thriftifyCounterSuperColumns(Collection<IColumn> columns, boolean reverseOrder)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column : columns)
        {
            List<CounterColumn> subcolumns = thriftifyCounterSubColumns(column.getSubColumns());
            if (subcolumns.isEmpty())
            {
                continue;
            }
            CounterSuperColumn superColumn = new CounterSuperColumn(column.name(), subcolumns);
            thriftSuperColumns.add(new ColumnOrSuperColumn().setCounter_super_column(superColumn));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

}

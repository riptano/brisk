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
package org.apache.cassandra;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;


/**
 * A test server that mainly throws errors.  Needed to test the CassandraProxyClientCode...
 *
 */
public class BriskErrorServer implements Brisk.Iface
{

    private AtomicInteger counter = new AtomicInteger(0);
 // thread local state containing session information
    public final ThreadLocal<ClientState> clientState = new ThreadLocal<ClientState>()
    {
        @Override
        public ClientState initialValue()
        {
            return new ClientState();
        }
    };

    public List<List<String>> describe_keys(String keyspace, List<ByteBuffer> keys) throws TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> arg0, ConsistencyLevel arg1)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub

    }

    public String describe_cluster_name() throws TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public KsDef describe_keyspace(String table) throws NotFoundException, InvalidRequestException, TException
    {
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(table);
        if (ksm == null)
            throw new NotFoundException();

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        for (CFMetaData cfm : ksm.cfMetaData().values())
            cfDefs.add(CFMetaData.convertToThrift(cfm));
        KsDef ksdef = new KsDef(ksm.name, ksm.strategyClass.getName(), cfDefs);
        ksdef.setStrategy_options(ksm.strategyOptions);
        return ksdef;
    }

    public List<KsDef> describe_keyspaces() throws InvalidRequestException, TException
    {
        Set<String> keyspaces = DatabaseDescriptor.getTables();
        List<KsDef> ksset = new ArrayList<KsDef>();
        for (String ks : keyspaces)
        {
            try
            {
                ksset.add(describe_keyspace(ks));
            }
            catch (NotFoundException nfe)
            {
            }
        }
        return ksset;
    }

    public String describe_partitioner() throws TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public List<TokenRange> describe_ring(String arg0) throws InvalidRequestException, TException
    {
        return null;
    }

    public Map<String, List<String>> describe_schema_versions() throws InvalidRequestException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String describe_snitch() throws TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> describe_splits(String arg0, String arg1, String arg2, int arg3) throws TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String describe_version() throws TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * This method tracks down how many times this method has been called.
     */
    public ColumnOrSuperColumn get(ByteBuffer arg0, ColumnPath arg1, ConsistencyLevel arg2)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException
    {
        counter.incrementAndGet();
        throw new TimedOutException();
    }

    public int get_count(ByteBuffer arg0, ColumnParent arg1, SlicePredicate arg2, ConsistencyLevel arg3)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return counter.incrementAndGet();
    }

    public List<KeySlice> get_indexed_slices(ColumnParent arg0, IndexClause arg1, SlicePredicate arg2,
            ConsistencyLevel arg3) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public List<KeySlice> get_range_slices(ColumnParent arg0, SlicePredicate arg1, KeyRange arg2, ConsistencyLevel arg3)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public List<ColumnOrSuperColumn> get_slice(ByteBuffer arg0, ColumnParent arg1, SlicePredicate arg2,
            ConsistencyLevel arg3) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void insert(ByteBuffer arg0, ColumnParent arg1, Column arg2, ConsistencyLevel arg3)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub

    }

    public void login(AuthenticationRequest arg0) throws AuthenticationException, AuthorizationException, TException
    {
        // TODO Auto-generated method stub

    }

    public Map<ByteBuffer, Integer> multiget_count(List<ByteBuffer> arg0, ColumnParent arg1, SlicePredicate arg2,
            ConsistencyLevel arg3) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> arg0, ColumnParent arg1,
            SlicePredicate arg2, ConsistencyLevel arg3) throws InvalidRequestException, UnavailableException,
            TimedOutException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void remove(ByteBuffer arg0, ColumnPath arg1, long arg2, ConsistencyLevel arg3)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub

    }

    public void set_keyspace(String arg0) throws InvalidRequestException, TException
    {
        // TODO Auto-generated method stub

    }

    public String system_add_column_family(CfDef arg0) throws InvalidRequestException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String system_add_keyspace(KsDef arg0) throws InvalidRequestException, TException
    {
        return "ok";
    }

    public String system_drop_column_family(String arg0) throws InvalidRequestException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String system_drop_keyspace(String arg0) throws InvalidRequestException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String system_update_column_family(CfDef arg0) throws InvalidRequestException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String system_update_keyspace(KsDef arg0) throws InvalidRequestException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void truncate(String arg0) throws InvalidRequestException, UnavailableException, TException
    {
        // TODO Auto-generated method stub

    }

    public void add(ByteBuffer arg0, ColumnParent arg1, CounterColumn arg2, ConsistencyLevel arg3)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub
        
    }

    public CqlResult execute_cql_query(ByteBuffer arg0, Compression arg1) throws InvalidRequestException,
            UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void remove_counter(ByteBuffer arg0, ColumnPath arg1, ConsistencyLevel arg2) throws InvalidRequestException,
            UnavailableException, TimedOutException, TException
    {
        // TODO Auto-generated method stub
        
    }

    public LocalOrRemoteBlock get_cfs_sblock(String callerHostName, ByteBuffer blockId, ByteBuffer sblockId, int offset,
    		StorageType storageType)
            throws InvalidRequestException, UnavailableException, TimedOutException, NotFoundException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String get_jobtracker_address() throws NotFoundException, TException
    {
        // TODO Auto-generated method stub
        return null;
    }

	@Override
	public String move_job_tracker(String new_jobtracker)
			throws NotFoundException, TException {
		// TODO Auto-generated method stub
		return null;
	}



}

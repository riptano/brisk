package org.apache.cassandra.hadoop.trackers;

import static com.datastax.brisk.BriskDBUtil.validateAndGetColumn;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.brisk.BriskSchema;

/**
 * Responsible for access the DB and manages Job Tracker information.
 * 
 * Acts on {@link BriskSchema#JOB_TRACKER_CF} column family
 * 
 * @see BriskSchema
 * 
 */
public class TrackerManager {

    private static final ByteBuffer currentJobtrackerKey = ByteBufferUtil.bytes("currentJobTracker");

    private static final ColumnParent cp = new ColumnParent(BriskSchema.JOB_TRACKER_CF);

    private static final ByteBuffer columnName = ByteBufferUtil.bytes("current");

    /** n It is the path to the only column in JOB_TRACKER_CF */
    private static final QueryPath jobTrackerCFQueryPath = new QueryPath(BriskSchema.JOB_TRACKER_CF, null, columnName);

    /**
     * Retrieves the current job tracker IP.
     * 
     * @return the current job tracker IP
     * @throws TrackerManagerException
     */
    public static InetAddress getCurrentJobtrackerLocation() throws TrackerManagerException {

        ReadCommand rc = new SliceByNamesReadCommand(BriskSchema.KEYSPACE_NAME, currentJobtrackerKey, cp, Arrays.asList(columnName));

        String result;
        try {
            List<Row> rows = StorageProxy.read(Arrays.asList(rc), ConsistencyLevel.QUORUM);
            IColumn col = validateAndGetColumn(rows, columnName);

            // ByteBuffer util duplicates for us the value.
            result = ByteBufferUtil.string(col.value());
            return InetAddress.getByName(result);

        } catch (NotFoundException e) {
            return null;
        } catch (Exception e) {
            throw new TrackerManagerException(e);
        }
    }

    /**
     * Insert the new Job Tracker location (IP)
     * 
     * @param jobTrackerIP
     * @throws TrackerManagerException if an error occurs
     */
    public static void insertJobtrackerLocation(InetAddress jobTrackerAddress) throws TrackerManagerException {
        // Insert the current JB location in the only column of the only row.
        RowMutation rm = new RowMutation(BriskSchema.KEYSPACE_NAME, currentJobtrackerKey);

        rm.add(jobTrackerCFQueryPath, ByteBufferUtil.bytes(jobTrackerAddress.getHostAddress()),
                System.currentTimeMillis());
        try {
            StorageProxy.mutate(Arrays.asList(rm), ConsistencyLevel.QUORUM);
        } catch (Exception e) {
            throw new TrackerManagerException(e);
        }
    }

}

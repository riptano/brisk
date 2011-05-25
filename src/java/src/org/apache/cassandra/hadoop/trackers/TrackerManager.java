package org.apache.cassandra.hadoop.trackers;

import static com.datastax.brisk.BriskDBUtil.validateAndGetColumn;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
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

	/**
	 * Retrieves the current job tracker IP.
	 * 
	 * @return the current job tracker IP
	 * @throws TrackerAdminException
	 */
	public static String getCurrentJobtrackerLocation() throws TrackerAdminException {

		ReadCommand rc = new SliceByNamesReadCommand(BriskSchema.KEYSPACE_NAME,
				currentJobtrackerKey, cp, Arrays.asList(columnName));

		String result;
		try {
			List<Row> rows = StorageProxy.read(Arrays.asList(rc), ConsistencyLevel.QUORUM);
			IColumn col = validateAndGetColumn(rows, columnName);

			// ByteBuffer util duplicates for us the value.
			result = ByteBufferUtil.string(col.value());

		} catch (NotFoundException e) {
			result = null;
		} catch (Exception e) {
			throw new TrackerAdminException(e);
		}

		return result;
	}

	/**
	 * Insert the new Job Tracker location (IP)
	 * @param jobTrackerIP
	 * @throws TrackerAdminException if an error occurs
	 */
	public static void insertJobtrackerLocation(String jobTrackerIP) throws TrackerAdminException {
		RowMutation rm = new RowMutation(BriskSchema.KEYSPACE_NAME, currentJobtrackerKey);
		try {
			StorageProxy.mutate(Arrays.asList(rm), ConsistencyLevel.QUORUM);
		} 
		catch (Exception e)
		{
			throw new TrackerAdminException(e);
		}
	}

}

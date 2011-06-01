include "cassandra.thrift"

namespace java org.apache.cassandra.thrift
namespace cpp org.apache.cassandra
namespace csharp Apache.Cassandra
namespace py cassandra
namespace php cassandra
namespace perl Cassandra
namespace rb CassandraThrift

#
# Exceptions
# (note that internal server errors will raise a TApplicationException, courtesy of Thrift)
#

/** A specific column was requested that does not exist. */
exception NotFoundException {
}

/** Invalid request could mean keyspace or column family does not exist, required parameters are missing, or a parameter is malformed.
    why contains an associated error message.
*/
exception InvalidRequestException {
    1: required string why
}

/** Not all the replicas required could be created and/or read. */
exception UnavailableException {
}

/** RPC timeout was exceeded.  either a node failed mid-operation, or load was too high, or the requested op was too large. */
exception TimedOutException {
}

/** Identifies what type of storage to use */
enum StorageType {
  CFS_REGULAR, CFS_ARCHIVE
}


struct LocalBlock
{
    1: required string file,
    2: required i64 offset,
    3: required i64 length
}

struct LocalOrRemoteBlock
{
    1: optional binary remote_block,
    2: optional LocalBlock local_block
}

service Brisk extends cassandra.Cassandra
{
  /**  returns (in order) the endpoints for each key specified. */
  list<list<string>> describe_keys(1:required string keyspace, 2:required list<binary> keys)
   throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /** returns a local or remote sub block
   *
   * A remote sub block is the expected binary sub block data
   *
   * A local sub block is the file, offset and length for the calling application to read
   * This is a great optimization because it avoids any actual data transfer.
   *
   */
   LocalOrRemoteBlock get_cfs_sblock(1:required string caller_host_name, 2:required binary block_id, 3:required binary sblock_id, 4:i32 offset=0, 5:required StorageType storageType)
    throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te, 4:NotFoundException nfe),


   /** returns the hostname:port of the jobtracker control port
    *
    */
    string get_jobtracker_address() throws (1:NotFoundException nfe)

    /**
     * Mode the JobTracker to the node specified in the parameter.
     */
    string move_job_tracker(1:string new_jobtracker) throws (1:NotFoundException nfe),

   /** Returns the subset of columns specified in SlicePredicate for the rows matching the IndexClause within key range
    *
    */
   list<cassandra.KeySlice> get_indexed_slices(1:required cassandra.ColumnParent column_parent,
                                    2:required cassandra.IndexClause index_clause,
                                    3:required cassandra.KeyRange key_range,
                                    4:required cassandra.SlicePredicate column_predicate,
                                    5:required cassandra.ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
    throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te)
}
About the Pig Demo 
------------------------ 

This pig demo consists of a data file (example.txt) containing tuples 
of usernames and scores. There are three use case files containing
pig commands:

* 001_sort-by-total-cfs.pig - Using data stored in the Brisk file system
  (CFS), performs MapReduce tasks to calculate the total score per user.
 
* 002_push-data-to-cassandra.pig - Transforms the raw score data into the
  format of a Cassandra column family and pushes the data over
  to Cassandra. Requires that you create a column family in 
  Cassandra first.

* 003_sort-by-total-cs.pig - Building on the previous use case, reads the
  data from a Cassandra column family and calculates the total score
  per user.

To run the demo, you must first load the demo data into CFS. To
run use cases 002-003, you must first create a keyspace and column 
family in Cassandra.  


Loading the Demo Data Into CFS
--------------------------------- 

The demo data file is located in ./files/example.txt.

To load the data file into CFS:

   brisk hadoop fs -put /usr/share/brisk-demos/pig/files/example.txt /

or in a binary distribution:

   brisk hadoop fs -put $BRISK_HOME/brisk/demos/pig/files/example.txt /


Creating the PigDemo Keyspace in Cassandra
---------------------------------------------

In order for Pig to access data in Cassandra, the target keyspace and
column family must already exist (Pig can read and write data from/to a
column family in Cassandra, but it will not create the column family if 
it does not already exist).

To create the "PigDemo" keyspace and "Scores" column family used in 
the demo, run the following commands in the "cassandra-cli" utility.

1. Start the "cassandra-cli" utility:

      cassandra-cli

   or in a binary distribution:

      $BRISK_HOME/brisk/resources/cassandra/bin/cassandra-cli

2. Connect to a node in your Brisk cluster on port 9160. For example:

      [default@unknown] connect 110.82.155.4/9160

   or if running on a single-node cluster as localhost:

      [default@unknown] connect localhost/9160


3. Create the PigDemo keyspace.

      [default@unknown] create keyspace PigDemo with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = [{replication_factor:1}];

4. Connect to the PigDemo keyspace you just created.

      [default@unknown] use PigDemo;

5. Create the Scores column family.

      [default@unknown] create column family Scores with comparator = 'LongType';

6. Exit cassandra-cli:

      [default@unknown] exit;

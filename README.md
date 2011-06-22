DataStax Brisk
==============

This package contains a HDFS compatable layer (CFS) and a CassandraJobConf
which can be used to run MR jobs without HDFS or dedicated job/task trackers.

It also includes a hive-driver for accessing data in cassandra as well as a
hive meta-store implementation.

Hadoop jobs and Hive are setup to work with MR cluster.

For detailed docs please see: 
    [http://www.datastax.com/docs/0.8/brisk/index](http://www.datastax.com/docs/0.8/brisk/index)

You can also discuss Brisk on freenode #datastax-brisk

Required Setup
==============

On linux systems, you need to run the following as root

    echo 1 > /proc/sys/vm/overcommit_memory

This is to avoid OOM errors when tasks are spawned.

Getting Started
===============

To try it out run:

1. compile and download all dependencies
  <pre>
      ant
  </pre>
2. start cassandra with built in job/task trackers
  <pre>
      ./bin/brisk cassandra -t
  </pre>
3. view jobtracker
  <pre>
      http://localhost:50030
  </pre>
4. examine CassandraFS
  <pre>
      ./bin/brisk hadoop fs -lsr cfs:///
  </pre>
5. start hive shell or webUI
  <pre>
     ./bin/brisk hive
  </pre>
   or
  <pre>
     ./bin/brisk hive --service hwi
  </pre>
open web browser to http://localhost:9999/hwi

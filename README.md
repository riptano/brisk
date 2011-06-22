DataStax Brisk
==============

This package contains a HDFS compatable layer (CFS) and a CassandraJobConf
which can be used to run MR jobs without HDFS or dedicated job/task trackers.

It also includes a hive-driver for accessing data in cassandra as well as a
hive meta-store implementation.

Hadoop jobs and Hive are setup to work with MR cluster.

For detailed docs please see: 
    http://www.datastax.com/docs/0.8/brisk/index

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

        ant

2. start cassandra with built in job/task trackers

        ./bin/brisk cassandra -t

3. view jobtracker

        http://localhost:50030

4. examine CassandraFS

        ./bin/brisk hadoop fs -lsr cfs:///

5. start hive shell or webUI

        ./bin/brisk hive

   or

        ./bin/brisk hive --service hwi

open web browser to http://localhost:9999/hwi

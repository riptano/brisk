#!/bin/sh

export CASSANDRA_HOME=`pwd`/`dirname $0`/../resources/cassandra

. $CASSANDRA_HOME/bin/cassandra.in.sh
 
for jar in `pwd`/`dirname $0`/../build/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

for jar in `pwd`/`dirname $0`/../lib/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

for jar in `pwd`/`dirname $0`/../resources/hive/lib/hive-cassandra*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH

#hadoop requires absolute home
export HADOOP_HOME=`pwd`/`dirname $0`/../resources/hadoop
#export HADOOP_LOG_DIR=

export PIG_HOME=`dirname $0`/../resources/pig
export PIG_CLASSPATH=$HADOOP_HOME/conf:$CLASSPATH
# pig also needs some of hadoop's dependencies
for jar in `ls $HADOOP_HOME/*.jar $HADOOP_HOME/lib/*.jar`; do
    PIG_CLASSPATH=$PIG_CLASSPATH:$jar
done

export HIVE_HOME=`dirname $0`/../resources/hive


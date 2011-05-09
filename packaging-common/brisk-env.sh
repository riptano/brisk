#!/bin/sh

export CASSANDRA_HOME=/usr/share/brisk/cassandra
export CASSANDRA_BIN=/usr/sbin

if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    for include in /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh \
                   ~/.cassandra.in.sh \
                   `dirname $0`/cassandra.in.sh; do
        if [ -r $include ]; then
            . $include
            break
        fi
    done
elif [ -r $CASSANDRA_INCLUDE ]; then
    . $CASSANDRA_INCLUDE
fi

# jasper hack
for jar in `find /usr/share/brisk/hadoop/lib/jasper*.jar`; do
    export CLASSPATH=$jar:$CLASSPATH
done
for jar in `find /usr/share/brisk/*/lib`; do
    export CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH

#hadoop requires absolute home
export HADOOP_HOME=/usr/share/brisk/hadoop
export HADOOP_BIN=/usr/bin
export HADOOP_LOG_DIR=/var/log/hadoop

#export PIG_HOME=
export PIG_CLASSPATH=$HADOOP_HOME/conf:$CLASSPATH

export HIVE_HOME=/usr/share/brisk/hive
export HIVE_BIN=/usr/bin


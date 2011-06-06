#!/bin/sh

if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=$(readlink -f `which java` | sed "s:bin/java::")
fi

export CASSANDRA_HOME=/usr/share/brisk/cassandra
export CASSANDRA_BIN=/usr/sbin

#
# Add brisk jar
#
export CLASSPATH=$CLASSPATH:/usr/share/brisk/brisk.jar

#
# Source cassandra env
#
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

#
# Initialize Hive env
#
export HIVE_HOME=/usr/share/brisk/hive
export HIVE_CONF_DIR=/etc/brisk/hive
export HIVE_BIN=$HIVE_HOME/bin
if [ -z "$HIVE_LOG_ROOT" ]; then
    if [ -w /var/log/hive ]; then
        export HIVE_LOG_ROOT=/var/log
    else
        export HIVE_LOG_ROOT=$HOME
    fi
fi

#
#Add hive cassandra driver
#
for jar in $HIVE_HOME/lib/hive-cassandra*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

#
# Initialize Pig env
#
export PIG_HOME=/usr/share/brisk/pig
export PIG_CONF_DIR=/etc/brisk/pig
# PIG_CLASSPATH needs to be set last

#hadoop requires absolute home
export HADOOP_HOME=/usr/share/brisk/hadoop
export HADOOP_CONF_DIR=/etc/brisk/hadoop
export HADOOP_BIN=$HADOOP_HOME/bin
if [ -z "$HADOOP_LOG_DIR" ]; then
    if [ -w /var/log/hadoop ]; then
        export HADOOP_LOG_DIR=/var/log/hadoop
    else
        export HADOOP_LOG_DIR=$HOME
    fi
fi

# needed for webapps
CLASSPATH=$CLASSPATH:$HADOOP_HOME:$HADOOP_HOME:/etc/brisk/hadoop

if [ -n "$HADOOP_NATIVE_ROOT" ]; then
    for jar in $HADOOP_NATIVE_ROOT/*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
    done

    JAVA_PLATFORM=`$HADOOP_HOME/bin/hadoop org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`

    export JAVA_LIBRARY_PATH=$HADOOP_NATIVE_ROOT/lib/native/${JAVA_PLATFORM}/
fi

#
# Add hadoop libs
#
for jar in $HADOOP_HOME/*.jar $HADOOP_HOME/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH
# pig script overrides the cp, so set this last
export PIG_CLASSPATH=$CLASSPATH

#make the hadoop command accessible
export PATH=$HADOOP_BIN:$PATH




#!/bin/sh

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -x $JAVA_HOME/bin/java ]; then
    JAVA=$JAVA_HOME/bin/java
else
    JAVA=`which java`
fi

if [ -z $BRISK_HOME ]; then
    abspath=$(cd ${0%/*} && echo $PWD)
    export BRISK_HOME=`dirname "$abspath"`
fi

#
# Set the logging root
#
if [ -z $BRISK_LOG_ROOT ]; then
    export BRISK_LOG_ROOT=$BRISK_HOME/logs
fi

#
# Initialize cassandra env
#
export CASSANDRA_HOME=$BRISK_HOME/resources/cassandra
export CASSANDRA_BIN=$CASSANDRA_HOME/bin

. $CASSANDRA_BIN/cassandra.in.sh
 
#
# Add brisk jars
#
for jar in $BRISK_HOME/build/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

for jar in $BRISK_HOME/lib/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

#
#Add hive cassandra driver
#
for jar in $BRISK_HOME/resources/hive/lib/hive-cassandra*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

#
# Initialize Hadoop env
#
export HADOOP_HOME=$BRISK_HOME/resources/hadoop
export HADOOP_BIN=$HADOOP_HOME/bin
export HADOOP_LOG_DIR=$BRISK_LOG_ROOT/hadoop


if [ -n "$HADOOP_NATIVE_ROOT" ]; then
    for jar in $HADOOP_NATIVE_ROOT/*.jar; do   
	export CLASSPATH=$CLASSPATH:$jar
    done

    JAVA_PLATFORM=`$HADOOP_HOME/bin/hadoop org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`

    export JAVA_LIBRARY_PATH=$HADOOP_NATIVE_ROOT/lib/native/${JAVA_PLATFORM}/
fi

#needed for webapps
CLASSPATH=$CLASSPATH:$HADOOP_HOME
CLASSPATH=$CLASSPATH:$HADOOP_HOME/conf
    
for jar in $HADOOP_HOME/*.jar $HADOOP_HOME/lib/*.jar $HADOOP_HOME/lib/jsp-2.1/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done 

export HADOOP_CLASSPATH=$CLASSPATH

#export PIG_HOME=
#export PIG_CLASSPATH=$HADOOP_HOME/conf:$CLASSPATH


#
# Initialize Hive env
#
export HIVE_HOME=$BRISK_HOME/resources/hive
export HIVE_BIN=$HIVE_HOME/bin


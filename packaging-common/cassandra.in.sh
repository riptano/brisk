
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/brisk/cassandra

# The java classpath (required)
CLASSPATH=$CLASSPATH:$CASSANDRA_CONF

for jar in /usr/share/brisk/cassandra/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done
CLASSPATH=$CLASSPATH:/usr/share/brisk/brisk.jar

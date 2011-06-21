-- load the score data into a pig relation
score_data = LOAD 'cfs:///example.txt' AS (name:chararray, score:long); 

-- define 3 field tuple of name (key), score (column name), value
-- use an empty string for column value, null would make it a delete

cassandra_tuple = FOREACH score_data GENERATE name, score, '' AS value;

-- group tuples by user

group_by_name = GROUP cassandra_tuple BY name PARALLEL 3;

-- create an aggregated row per user containing tuples of scores

aggregate_scores = FOREACH group_by_name GENERATE group, cassandra_tuple.(score, value);

-- to see how the row is constructed you could output aggregate_scores
-- DUMP aggregate_scores; 

-- push the rows to a Cassandra column family
-- the keyspace and column family must exist in Cassandra (see README)

STORE aggregate_scores INTO 'cassandra://PigDemo/Scores' using CassandraStorage();

data = LOAD 'cfs:///example.txt' using PigStorage() as (name:chararray, score:long);
-- empty string will be the column value, null would make it a delete
triples = FOREACH data generate name, score, '' as value;
grouped = GROUP triples by name PARALLEL 3;
aggregated = FOREACH grouped GENERATE group, triples.(score, value);
STORE aggregated INTO 'cassandra://Demo1/Scores' using CassandraStorage();

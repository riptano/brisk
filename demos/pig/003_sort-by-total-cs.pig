-- this use case builds on 002_push-data-to-cassandra.pig

-- load the score data into a pig relation from Cassandra
-- the schema is inferred from the column family comparator/validator

cassandra_data = LOAD 'cassandra://PigDemo/Scores' USING CassandraStorage() AS (name, columns: bag {T: tuple(score, value)});

-- calculate the total score per user
-- the users are already grouped by Cassandra row

total_scores = FOREACH cassandra_data GENERATE name, COUNT(columns.score), LongSum(columns.score) as total PARALLEL 3;

-- order the results by score in descending order

ordered_scores = ORDER total_scores BY total DESC PARALLEL 3;

-- output results to standard output

DUMP ordered_scores;

-- or if you wanted to store the output back to CFS
-- STORE ordered_scores INTO 'cfs:///final_scores.txt' USING PigStorage();



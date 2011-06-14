-- we don't have to specify the schema, it will be inferred from the CF comparator/validator
data = LOAD 'cassandra://Demo1/Scores' using CassandraStorage() AS (name, columns: bag {T: tuple(score, value)});
-- notice that we don't need to group now, we did that in the conversion
counted = FOREACH data GENERATE name, COUNT(columns.score), LongSum(columns.score) as total PARALLEL 3;
ordered = ORDER counted by total desc PARALLEL 3;
-- STORE ordered INTO 'cfs:///output2.txt' using PigStorage();
DUMP ordered;

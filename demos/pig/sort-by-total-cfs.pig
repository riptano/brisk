data = LOAD 'cfs:///example.txt' using PigStorage() as (name:chararray, score:long);
-- parallel keyword controls how many reducers are used
grouped = GROUP data by name PARALLEL 3;
counted = FOREACH grouped GENERATE group, COUNT(data.name), LongSum(data.score) as total;
ordered = ORDER counted by total desc PARALLEL 3;
dump ordered;
-- we could also store the output
-- STORE ordered INTO 'cfs:///output.txt' using PigStorage();

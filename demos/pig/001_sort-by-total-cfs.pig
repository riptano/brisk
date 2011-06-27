-- load the score data into a pig relation
score_data = LOAD 'cfs:///example.txt' USING PigStorage() AS (name:chararray, score:long);

-- group tuples by user
-- the PARALLEL keyword controls how many reducers are used

name_group = GROUP score_data BY name PARALLEL 3;

-- calculate the total score per user

name_total = FOREACH name_group GENERATE group, COUNT(score_data.name), LongSum(score_data.score) AS total_score;

-- order the results by score in descending order

ordered_scores = ORDER name_total BY total_score DESC PARALLEL 3;

-- output results to standard output

DUMP ordered_scores;

-- or if you wanted to store the output back to CFS
-- STORE ordered_scores INTO 'cfs:///final_scores.txt' USING PigStorage();

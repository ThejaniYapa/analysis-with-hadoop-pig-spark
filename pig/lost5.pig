results= LOAD 'hdfs://c1de6cc21ae5:9000/user/root/inputData/results.csv' USING PigStorage(',') as 
(date:datetime, home_team:chararray, away_team:chararray, home_score:int, away_score:int, tournament:chararray, city:chararray, country:chararray, neutral:boolean);
outcome = FOREACH results  GENERATE  tournament, city,( CASE
    WHEN home_score > away_score THEN away_team
    WHEN home_score < away_score THEN home_team
    WHEN home_score == away_score THEN null
  END
)as lostteam:chararray;
lost_filtered = FILTER outcome BY lostteam is not null;
lost_team = GROUP lost_filtered BY $2; 
lost_count = FOREACH lost_team GENERATE group, COUNT(lost_filtered.lostteam) AS lostcount:int;
DUMP lost_count;
Lost_sorted = order lost_count by lostcount desc;
Lost5= limit Lost_sorted 5;
DUMP Lost5;
store Lost5 into 'hdfs://c1de6cc21ae5:9000/user/root/outputData/223lost5';

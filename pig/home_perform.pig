results= LOAD 'hdfs://c1de6cc21ae5:9000/user/root/inputData/results.csv' USING PigStorage(',') as 
(date:datetime, home_team:chararray, away_team:chararray, home_score:int, away_score:int, tournament:chararray, city:chararray, country:chararray, neutral:boolean);
res_hometeam = GROUP results BY  home_team;
home_performance= foreach res_hometeam generate group, SUM(results.home_score) AS tot_scored:int , SUM(results.away_score) AS tot_concded:int ;
DUMP home_performance;
store home_performance into 'hdfs://c1de6cc21ae5:9000/user/root/outputData/home_performance';

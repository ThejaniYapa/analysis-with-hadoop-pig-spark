results= LOAD 'hdfs://c1de6cc21ae5:9000/user/root/inputData/results.csv' USING PigStorage(',') as 
(date:datetime, home_team:chararray, away_team:chararray, home_score:int, away_score:int, tournament:chararray, city:chararray, country:chararray, neutral:boolean);
res_awayteam = GROUP results BY  away_team;
avg_awyscore = FOREACH res_awayteam GENERATE group, AVG(results.away_score) AS avg_awayscore:double;
sorted_avg_awyscore = ORDER avg_awyscore BY avg_awayscore desc;
top10 = LIMIT sorted_avg_awyscore 10;
DUMP top10;
store top10 into 'hdfs://c1de6cc21ae5:9000/user/root/outputData/222top10';

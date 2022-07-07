## Dataset
This dataset includes 41,586 results of international football matches starting from the very first official match in 1872 up to 2020. You can find the complete dataset in the results.zip
# Columns of the dataset
● date - date of the match
● home_team - the name of the home team
● away_team - the name of the away team
● home_score - full-time home team score including extra time, not including penalty-shootouts
● away_score - full-time away team score including extra time, not including penalty-shootouts
● tournament - the name of the tournament
● city - the name of the city/town/administrative unit where the match was played
● country - the name of the country where the match was played
● neutral - TRUE/FALSE column indicating whether the match was played at a neutral venue

## 1 The analysis based on football matches dataset provides high-level summarizations and make predictions based on
the findings of the analysis.

# 1.1  Using Hadoop MapReduce
1. The total number of matches played, ended with a result and drawn.
Code:       MapReduce\src\main\java\bdpCW\TotalCount
Execution:  yarn jar MapReduce\target\mapreduce-1.0-SNAPSHOT.jar bdpMP.TotalCount.TotalCount inputData/ output/totcount
Output:     hdfs dfs -cat output/totcount /part-m-00000
Result:     outputData\totalcount.txt
    Records	41642

Code:       MapReduce\src\main\java\bdpMP\CountByOutcome\CountByOutcome.java
Execution:  yarn jar MapReduce/target/mapreduce-1.0-SNAPSHOT.jar bdpMP.CountByOutcome.CountByOutcome inputData/ output/countOutcome
Output:     hdfs dfs -cat output/countOutcome/part-r-00000
Result:     outputData\countOutcome.txt
    Draw	9592
    Result	32048

2. Total number of matches played in each city
Code:       MapReduce\src\main\java\bdpMP\countbyCity\countbyCity.java
Jar:        mapreduce-1.0-SNAPSHOT.jar
Execution:  yarn jar MapReduce/target/mapreduce-1.0-SNAPSHOT.jar bdpMP.countbyCity.countbyCity inputData/ output/countbyCity
Output:     hdfs dfs -cat output/countbyCity/part-m-00000
Result:     outputData\countByCity.txt

# 1.2 using Pig 
1. The total number of goals scored and conceded by each team when played home.

Code:       Pig Script pig\home_perform.pig
Execution:  pig -x mapreduce
            exec home_perform.pig
            quit
            hdfs dfs -ls /user/root/outputData/home_perf
            hdfs dfs -get /user/root/outputData/home_perf/part-r-00000 
            /resource/outputData/home_perf.txt
Result:     outputData\home_perf.txt

2. Top 10 teams based on the average goals per match when played away.
Code:       pig\top10.pig
Execution:  pig -x mapreduce
            exec top10.pig
            quit
            hdfs dfs -ls /user/root/outputData/top10
            hdfs dfs -get /user/root/outputData/top10/part-r-00000 /resource/outputData/top10.txt
Result:     outputData\top10.txt

3. The 5 teams who have lost the most number of matches.
Code:        pig\lost5.pig
Execution:  pig -x mapreduce
            exec lost5.pig
            quit
            hdfs dfs -ls /user/root/outputData/lost5
            hdfs dfs -get /user/root/outputData/lost5/part-r-00000 /resource/outputData/lost5.txt
Result:     outputData\lost5.txt



# 1.3 using Spark 
1. Percentage of countries that have hosted more than one type of tournament.
2. Histogram of the total number of matches played overtime in year granularity.
3. The total number of matches won, lost, and drawn by each team.

## 2 Performing Machine Learning model using Spark MLlib
Build a model that predicts if a given team will win or lose the match given the rival and the
venue. (e.g., predict who will win if Argentina plays against Brazil at Milan, Italy). Use 80% of
the data for training and 20% of the data for validation. Clearly state the steps you’ve
followed for your analysis


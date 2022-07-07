/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package bdpCW.CountByOutcome;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CountByOutcome {
    public static class RecordCount
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        public static String OUTCOME_GROUP;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            try {
            int homeScore = Integer.parseInt(words[3]);
            int awayScore = Integer.parseInt(words[4]);


            if (homeScore==awayScore) {
                //context.getCounter(,"Draw")
                //        .increment(1);
                OUTCOME_GROUP= "Draw";
                context.getCounter(OUTCOME_GROUP, "Draw")
                        .increment(1);
            } else {
                OUTCOME_GROUP= "Result";
                context.getCounter(OUTCOME_GROUP, "Result")
                        .increment(1);
                //context.write(new Text(context.getCurrentValue()),new LongWritable(context.getCurrentKey()));
            }

            }catch(NumberFormatException ex){
                System.err.println("Invalid string in argument");
                //request for well-formatted string
            }

        }
    }


    /*
    @Override
    public void setup(Mapper.Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        Job currentJob = cluster.getJob(context.getJobID());
        fileReducer.mapperCounter = currentJob.getCounters().findCounter(RecordCount.OUTCOME_GROUP).getValue();
    }*/
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: CountNumUsersByState <users> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "CountNum");
        job.setJarByClass(CountByOutcome.class);

        job.setMapperClass(RecordCount.class);
        job.setNumReduceTasks(0);
        //job.setReducerClass(CountByOutcome.fileReducer.class);

        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        //MapFileOutputFormat.setOutputPath(job,outputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        List<String> outcome = Arrays.asList("DRAW","RESULT");


        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(String.valueOf(outcome)) ){
                System.out.println(counter.getDisplayName() + "\t"
                        + counter.getValue());
            }
        }

        // Clean up empty output directory
        //FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }
}

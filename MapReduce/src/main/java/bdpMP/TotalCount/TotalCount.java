package bdpCW.TotalCount;

import java.io.IOException;

import bdpCW.CountByOutcome.CountByOutcome;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TotalCount {

    public static class RecordCount
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text record = new Text("Record");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(record, one);
        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args)
                    .getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: WordCount <in> <out>");
                System.exit(2);
            }

            Path input = new Path(otherArgs[0]);
            Path outputDir = new Path(otherArgs[1]);

            Job job = Job.getInstance(conf, "Record count");
            job.setJarByClass(TotalCount.class);


            job.setMapperClass(RecordCount.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, outputDir);

            int code = job.waitForCompletion(true) ? 0 : 1;
            System.out.println(code);


            if (code == 0) {
                System.out.println( job.getCounters()+ "\t"
                            + job.getCounters().countCounters());
            }
            System.exit(code);

        }
    }


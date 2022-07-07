/*
 * Licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package bdpCW.countbyCity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

public class CountByCity {
    public static class CountByCityMapper
                    extends Mapper<LongWritable, Text, Text, IntWritable> {
                public static String CITY;
                //public static List<String> outcome;
        /* protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            org.apache.hadoop.mapreduce.counters.Limits.init();
            }
        }*/
                @Override
                public void map(LongWritable key, Text value, Context context)
                        throws IOException, InterruptedException {
                    String line = value.toString();
                    String[] words = line.toUpperCase().split(",");
                    String city = words[6];
                    CITY = city;
                    if(city!=null) {
                        context.getCounter(CITY, city).increment(1);
                        //outcome.add(city);
                    }
                }
            }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /*conf.set ("Mapreduce.job.counters.limit", "2100");
        conf.set ("mapreduce.job.counters.max", "2100");
        conf.set ("mapreduce.job.counters.groups.max=2500", "2100");*/
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: CountNumUsersByState <users> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "Count Num Matches By Result");
        job.setJarByClass(CountByCity.class);

        job.setMapperClass(CountByCityMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;
        /*for (Counter counter : job.getCounters().getGroup(
                String.valueOf(CountNumUsersByStateMapper.outcome))){
                        //CountNumUsersByStateMapper.CITY)) {
                    System.out.println(counter.getDisplayName() + "\t"
                            + counter.getValue());
                }*/
        // Clean up empty output directory
        //FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }
}

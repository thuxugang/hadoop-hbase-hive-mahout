/**
 * Created by xugang on 2016/11/1.
 */
package mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CounterMR {
    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] val = value.toString().split("\\s+");
            if(val.length < 2)
                context.getCounter("ErrorCounter", "<2").increment(1);
            else if(val.length > 2)
                context.getCounter("ErrorCounter", ">2").increment(1);
            context.write(key, value);
        }
    }
}

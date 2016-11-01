/**
 * Created by xugang on 2016/11/1.
 */

package mapreduce;

import jar.MakeJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;

//统计商品总数
public class PartitionerMR {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] s = value.toString().split("\\s+");
            context.write(new Text(s[0]), new IntWritable(Integer.parseInt(s[1])));
        }
    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class MyPartitioner extends Partitioner<Text, IntWritable>{

        //numPartitions:reduce个数
        public int getPartition(Text key, IntWritable value, int numPartitions) {

            if(key.toString().equals("shoes")){
                return 0;
            }
            if(key.toString().equals("hat")){
                return 0;
            }
            return 1;

        }
    }

    public static void main(String[] args) throws Exception {

        // 打包
        File jarFile = MakeJar.createTempJar("target/classes");

        Configuration conf = new Configuration();
        Job job = new Job(conf, "partitioner");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(WordCountMR.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        System.out.print(jarFile.toString());
        job.setMapperClass(MyMapper.class); //为job设置Mapper类
        job.setReducerClass(MyReducer.class); //为job设置Reduce类
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);        //设置输出key的类型
        job.setOutputValueClass(IntWritable.class);//  设置输出value的类型

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //处理多个input
        MultipleInputs.addInputPath(job, new Path("/test/parta.txt"), TextInputFormat.class, MyMapper.class);
        MultipleInputs.addInputPath(job, new Path("/test/partb.txt"), TextInputFormat.class, MyMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("/test/part_r.txt"));//为map-reduce任务设置OutputFormat实现类  设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        jarFile.delete();
    }
}

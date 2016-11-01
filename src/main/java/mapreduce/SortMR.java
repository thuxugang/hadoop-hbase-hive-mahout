/**
 * Created by xugang on 2016/11/1.
 */
package mapreduce;

import jar.MakeJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

//利用shuffle排序
public class SortMR {

    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split("\\s+");
            context.write(new LongWritable(Long.parseLong(s[0])), NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

        public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class MyPartitioner extends Partitioner<LongWritable, NullWritable> {

        //numPartitions:reduce个数
        public int getPartition(LongWritable key, NullWritable value, int numPartitions) {
            if(key.get() <= 10){
                return 0;
            }else {
                return 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // 打包
        File jarFile = MakeJar.createTempJar("target/classes");

        Configuration conf = new Configuration();
        Job job = new Job(conf, "SortMR");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(SortMR.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        System.out.print(jarFile.toString());
        job.setMapperClass(MyMapper.class); //为job设置Mapper类
        job.setReducerClass(MyReducer.class); //为job设置Reduce类
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(LongWritable.class);        //设置输出key的类型
        job.setOutputValueClass(NullWritable.class);//  设置输出value的类型

        FileInputFormat.addInputPath(job, new Path("/test/record.txt")); //为map-reduce任务设置InputFormat实现类   设置输入路径
        FileOutputFormat.setOutputPath(job, new Path("/test/record_sort_r.txt"));//为map-reduce任务设置OutputFormat实现类  设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        jarFile.delete();
    }
}

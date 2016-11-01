/**
 * Created by xugang on 2016/10/27.
 */

package hbase;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import jar.MakeJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FileToHBaseMR {
    public static class FileToHBaseMRMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);// 输出<key,value>为<word,one>
            }
        }
    }
    public static class FileToHBaseMRReducer extends
            TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {// 遍历求和
                sum += val.get();
            }
            Put put = new Put(key.getBytes());//put实例化，每一个词存一行
            //列族为content,列修饰符为count，列值为数目
            put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
            context.write(new ImmutableBytesWritable(key.getBytes()), put);// 输出求和后的<key,value>
        }
    }
    public static void main(String[] args) throws Exception {
        String tablename = "wordcount";
        File jarFile = MakeJar.createTempJar("target/classes");
        Configuration conf_hadoop = new Configuration();

        Configuration conf_hbase = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf_hbase);
        if (admin.tableExists(tablename)) {
            System.out.println("table exists!recreating.......");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        HTableDescriptor htd = new HTableDescriptor(tablename);
        HColumnDescriptor tcd = new HColumnDescriptor("content");
        htd.addFamily(tcd);//创建列族
        admin.createTable(htd);//创建表

        Job job = new Job(conf_hadoop, "FileToHBaseMR");
        job.setJarByClass(FileToHBaseMR.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        //使用WordCountHbaseMapper类完成Map过程；
        job.setMapperClass(FileToHBaseMRMapper.class);
        TableMapReduceUtil.initTableReducerJob(tablename, FileToHBaseMRReducer.class, job);
        //设置任务数据的输入路径；
        FileInputFormat.addInputPath(job, new Path("/test/wc.txt")); //为map-reduce任务设置InputFormat实现类   设置输入路径
        //设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；
        job.setOutputKeyClass(Text.class);
        //设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为IntWritable；
        job.setOutputValueClass(IntWritable.class);
        //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        jarFile.delete();

    }
}

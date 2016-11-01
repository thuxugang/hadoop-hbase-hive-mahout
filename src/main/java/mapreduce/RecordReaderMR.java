/**
 * Created by xugang on 2016/11/1.
 */

package mapreduce;

import jar.MakeJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.File;
import java.io.IOException;

//分别统计单双数行和
public class RecordReaderMR {

    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MyReducer extends Reducer<LongWritable, Text, Text,IntWritable> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            Text write_key = new Text();
            IntWritable write_value = new IntWritable();
            //偶数行
            if(key.get() == 1){
                write_key.set("偶数行之和:");
            }else {
                write_key.set("奇数行之和:");
            }
            write_value.set(sum);
            context.write(write_key, write_value);
        }
    }

    public static class MyPartitioner extends Partitioner<LongWritable, Text> {

        //numPartitions:reduce个数
        public int getPartition(LongWritable key, Text value, int numPartitions) {
            //偶数行
            if(key.get() % 2 == 0) {
                key.set(1);
                return 1;
            }else{
                key.set(0);
                return 0;
            }
        }

    }

    //按行读取,key行号,value数值
    public static class MyRecordReader extends RecordReader<LongWritable, Text>{

        private long start;
        private long end;
        //行号
        private long pos;
        private FSDataInputStream fin = null;
        private LongWritable key = null;
        private Text value = null;
        private LineReader lineReader= null;

        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) inputSplit;
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Configuration conf = context.getConfiguration();
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            fin = fs.open(path);
            //指针指向开始
            fin.seek(start);
            lineReader = new LineReader(fin);
            pos = 1;
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(key == null)
                key = new LongWritable();
            key.set(pos);
            if(value == null)
                value = new Text();
            if(lineReader.readLine(value) == 0)
                return false;
            pos++;
            return true;
        }

        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        public void close() throws IOException {
            fin.close();
        }
    }

    public static class MyFileInputFormat extends FileInputFormat<LongWritable, Text>{

        public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new MyRecordReader();
        }

    }

    public static void main(String[] args) throws Exception {

        // 打包
        File jarFile = MakeJar.createTempJar("target/classes");

        Configuration conf = new Configuration();
        Job job = new Job(conf, "recordReader");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(RecordReaderMR.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

        job.setMapperClass(MyMapper.class); //为job设置Mapper类
        job.setReducerClass(MyReducer.class); //为job设置Reduce类
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);

        job.setInputFormatClass(MyFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/test/record.txt")); //为map-reduce任务设置InputFormat实现类   设置输入路径
        FileOutputFormat.setOutputPath(job, new Path("/test/record_r.txt"));//为map-reduce任务设置OutputFormat实现类  设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        jarFile.delete();
    }
}

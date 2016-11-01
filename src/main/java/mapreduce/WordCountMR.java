/**
 * Created by xugang on 2016/10/26.
 */
package mapreduce;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import jar.MakeJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMR {

    /**
     * MapReduceBase类:实现了Mapper和Reducer接口的基类（其中的方法只是实现接口，而未作任何事情）
     * Mapper接口：
     * WritableComparable接口：实现WritableComparable的类可以相互比较。所有被用作key的类应该实现此接口。
     * Reporter 则可用于报告整个应用的运行进度，本例中未使用。
     *
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        /**
         * LongWritable, IntWritable, Text 均是 Hadoop 中实现的用于封装 Java 数据类型的类，这些类实现了WritableComparable接口，
         * 都能够被串行化从而便于在分布式环境中进行数据交换，你可以将它们分别视为long,int,String 的替代品。
         */
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();//Text 实现了BinaryComparable类可以作为key值

        /**
         * Mapper接口中的map方法：
         * void map(K1 key, V1 value, OutputCollector<K2,V2> output, Reporter reporter)
         * 映射一个单个的输入k/v对到一个中间的k/v对
         * 输出对不需要和输入对是相同的类型，输入对可以映射到0个或多个输出对。
         * OutputCollector接口：收集Mapper和Reducer输出的<k,v>对。
         * OutputCollector接口的collect(k, v)方法:增加一个(k,v)对到output
         */

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            /**
             * 原始数据：
             * c++ java hello
             world java hello
             you me too
             map阶段，数据如下形式作为map的输入值：key为偏移量
             0  c++ java hello
             16 world java hello
             34 you me too

             */

            /**
             * 以下解析键值对
             * 解析后以键值对格式形成输出数据
             * 格式如下：前者是键排好序的，后者数字是值
             * c++ 1
             * java 1
             * hello 1
             * world 1
             * java 1
             * hello 1
             * you 1
             * me 1
             * too 1
             * 这些数据作为reduce的输出数据
             */
            StringTokenizer itr = new StringTokenizer(value.toString());//得到什么值
            System.out.println("value什么东西 ： "+value.toString());
            System.out.println("key什么东西 ： "+key.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());

                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        /**
         * reduce过程是对输入数据解析形成如下格式数据：
         * (c++ [1])
         * (java [1,1])
         * (hello [1,1])
         * (world [1])
         * (you [1])
         * (me [1])
         * (you [1])
         * 供接下来的实现的reduce程序分析数据数据
         *
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            /**
             * 自己的实现的reduce方法分析输入数据
             * 形成数据格式如下并存储
             *     c++    1
             *    hello   2
             *    java    2
             *    me      1
             *    too     1
             *    world   1
             *    you     1
             *
             */
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        /**
         * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
         * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等
         */
        // 打包
        File jarFile = MakeJar.createTempJar("target/classes");


        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        //这里需要配置参数即输入和输出的HDFS的文件路径
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: wordcount <in> <out>");
//            System.exit(2);
//        }

        Job job = new Job(conf, "word count");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(WordCountMR.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        System.out.print(jarFile.toString());
        job.setMapperClass(TokenizerMapper.class); //为job设置Mapper类
        job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类
        job.setReducerClass(IntSumReducer.class); //为job设置Reduce类
        job.setOutputKeyClass(Text.class);        //设置输出key的类型
        job.setOutputValueClass(IntWritable.class);//  设置输出value的类型

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //处理多个input
//        MultipleInputs.addInputPath(job, path, TextInputFormat.class, Mapper1.class);
        FileInputFormat.addInputPath(job, new Path("/test/wc.txt")); //为map-reduce任务设置InputFormat实现类   设置输入路径
        FileOutputFormat.setOutputPath(job, new Path("/test/wc_2.txt"));//为map-reduce任务设置OutputFormat实现类  设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        jarFile.delete();
    }

}
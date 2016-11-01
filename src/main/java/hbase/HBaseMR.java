/**
 * Created by xugang on 2016/10/27.
 */
package hbase;


import java.io.File;
import java.io.IOException;

import jar.MakeJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class HBaseMR {

    public static class HBaseMRMapper extends TableMapper<Text, Text>{

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            Text k = new Text(Bytes.toString(key.get()));
            Text v = new Text(value.getValue(Bytes.toBytes("basicInfo"),
                    Bytes.toBytes("age")));
            //年龄  人名
            context.write(v, k);
        }
    }

    public static class HBaseMRReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s,
                              Context context)
                throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(k2.toString()));
            for (Text v2 : v2s) {//遍历获得所有的人名
                //列族 列  值
                put.add(Bytes.toBytes("f1"), Bytes.toBytes(v2.toString()),
                        Bytes.toBytes(v2.toString()));
            }
            context.write(null, put);
        }
    }

    public static void main(String[] args){

        try {
            File jarFile = MakeJar.createTempJar("target/classes");
            Configuration conf = new Configuration();
            Job job = new Job(conf, "HBaseMR");
            job.setJarByClass(HBaseMR.class);
            ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
            System.out.println(jarFile.toString());
            Scan scan = new Scan();
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs

            TableMapReduceUtil.initTableMapperJob("students", scan, HBaseMRMapper.class, Text.class, Text.class, job);
            TableMapReduceUtil.initTableReducerJob("students_age", HBaseMRReducer.class, job);

            job.waitForCompletion(true);

            jarFile.delete();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}




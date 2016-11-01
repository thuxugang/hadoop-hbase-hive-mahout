package hbase;
/**
 * Created by xugang on 2016/10/28.
 */

import jar.MakeJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 本代码为批量离线 --- indexbuilder by MR
 * 实时更新用 --- hindex by 华为
 *
 */
public class IndexBuilderMR {


    public static class IndexBuilderMapper extends TableMapper<ImmutableBytesWritable, Put>{

        private Map<byte[], ImmutableBytesWritable> indexs = new HashMap<byte[], ImmutableBytesWritable>();
        private String columnFamily;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String tableName = conf.get("tableName");
            columnFamily = conf.get("columnFamily");
            String[] qualifiers = conf.getStrings("qualifiers");

            for(String q : qualifiers){
                indexs.put(Bytes.toBytes(q), new ImmutableBytesWritable(Bytes.toBytes(tableName+"-"+q)));
            }

        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Set<byte[]> keys = indexs.keySet();
            for(byte[] k : keys){
                ImmutableBytesWritable indexTableName = indexs.get(k);
                byte[] val = value.getValue(Bytes.toBytes(columnFamily), k);
                if(val != null){
                    Put put = new Put(val);
                    put.add(Bytes.toBytes("f1"), Bytes.toBytes("ID"), key.get());
                    context.write(indexTableName, put);
                }
            }
        }
    }
    public static void main(String[] args){

        Configuration conf = new Configuration();

        String tableName = "indexbuilder1";
        String columnFamily = "f1";
        String[] qualifiers = new String[]{"name"};

        conf.set("tableName",tableName);
        conf.set("columnFamily",columnFamily);
        conf.setStrings("qualifiers",qualifiers);

        try {
            File jarFile = MakeJar.createTempJar("target/classes");
            Job job = new Job(conf, tableName);
            job.setJarByClass(IndexBuilderMR.class);
            ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
            job.setMapperClass(IndexBuilderMapper.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(TableInputFormat.class);
            //可输出多个table
            job.setOutputFormatClass(MultiTableOutputFormat.class);

            Scan scan = new Scan();
            scan.setCaching(1000);

            TableMapReduceUtil.initTableMapperJob(tableName, scan, IndexBuilderMapper.class, ImmutableBytesWritable.class, Put.class, job);

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

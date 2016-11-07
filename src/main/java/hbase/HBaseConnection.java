/**
 * Created by xugang on 2016/10/27.
 */
package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseConnection {

    private Configuration cfg;
    private HConnection hConn;

    private HBaseConnection(){
        cfg = HBaseConfiguration.create();
        try {
            hConn = HConnectionManager.createConnection(cfg);
            System.out.print("=============================="+cfg.get("hbase.rootdir"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void creatTable(String tableName, List<String> cols){
        try {
            HBaseAdmin admin = new HBaseAdmin(cfg);
            if(admin.tableExists((tableName)))
                throw new IOException("table exists");
            else{
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for(String col : cols){
                    HColumnDescriptor colDesc = new HColumnDescriptor(col);
                    colDesc.setCompressionType(Compression.Algorithm.GZ);
                    colDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
                    tableDesc.addFamily(colDesc);
                }
                admin.createTable(tableDesc);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void saveData(String tableName, List<Put> puts){
        try {
            HTableInterface table = hConn.getTable(tableName);
            table.put(puts);
            table.setAutoFlush(false);
            table.flushCommits();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Result getData(String tableName, String rowkey){
        try {
            HTableInterface table = hConn.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowkey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void format(Result result){
        String rowkey = Bytes.toString(result.getRow());
        KeyValue[] kvs = result.raw();
        for(KeyValue kv : kvs){
            String family = Bytes.toString(kv.getFamily());
            String qualifier = Bytes.toString(kv.getQualifier());
            String value = Bytes.toString(kv.getValue());

            System.out.println("rowkey->" + rowkey + ",family->" + family + ",qualifier->" + qualifier + ",value->" + value);
        }
    }

    //全表扫描
    public void hbaseScan(String tableName){
        Scan scan = new Scan();
        //设置缓存,每次取1000条
        scan.setCaching(1000);
        try {
            HTableInterface table = hConn.getTable(tableName);
            ResultScanner scanner = table.getScanner(scan);
            for(Result res : scanner){
                format(res);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //设置过滤器
    public void filterTest(String tableName){
        Scan scan = new Scan();
        //设置缓存,每次取1000条
        scan.setCaching(1000);
        //RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Tom")));
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("J\\w+"));
        scan.setFilter(filter);
        try {
            HTableInterface table = hConn.getTable(tableName);
            ResultScanner scanner = table.getScanner(scan);
            for(Result res : scanner){
                format(res);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]){
        //建立连接
        HBaseConnection conn = new HBaseConnection();
        //创建表和列族
//        List<String> cols = new LinkedList<String>();
//        cols.add("basicInfo");
//        cols.add("moreInfo");
//        conn.creatTable("students",cols);
        //保存数据
//        List<Put> puts = new LinkedList<Put>();
//        Put put1 = new Put(Bytes.toBytes("Tom"));
//        put1.add(Bytes.toBytes("basicInfo"), Bytes.toBytes("age"), Bytes.toBytes("27"));
//        put1.add(Bytes.toBytes("moreInfo"), Bytes.toBytes("tel"), Bytes.toBytes("111"));
//        Put put2 = new Put(Bytes.toBytes("Jim"));
//        put2.add(Bytes.toBytes("basicInfo"), Bytes.toBytes("age"), Bytes.toBytes("28"));
//        put2.add(Bytes.toBytes("moreInfo"), Bytes.toBytes("tel"), Bytes.toBytes("112"));
//        puts.add(put1);
//        puts.add(put2);
//        conn.saveData("students",puts);
        //读取数据
//        Result result = conn.getData("students","Tom");
//        conn.format(result);
        //扫描
        conn.hbaseScan("students");
        //过滤器扫描
//        conn.filterTest("students");



    }

}

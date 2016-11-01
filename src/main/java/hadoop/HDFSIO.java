/**
 * Created by xugang on 2016/11/1.
 */
package hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class HDFSIO {

    public static void HDFSMKdir() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/iotest");
        fs.mkdirs(path);
        fs.close();
    }

    //SequenceFile
    public static void HDFSCreateFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/iotest/a.txt");
        FSDataOutputStream out = fs.create(path);
        out.write("hello hadoop".getBytes());
        fs.close();
    }

    public static void HDFSRenameFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/iotest/a.txt");
        Path newPath = new Path("/iotest/b.txt");
        fs.rename(path,newPath);
        fs.close();
    }

    //SequenceFile
    public static void HDFSUpload() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        InputStream in = new BufferedInputStream(new FileInputStream(new File("C:/xugang/test/wc.txt")));
        FSDataOutputStream out = fs.create(new Path("/iotest/wc.txt"));
        IOUtils.copyBytes(in, out, 4096);
        fs.close();
    }

    public static void main(String[] args){
        try {
            HDFSUpload();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

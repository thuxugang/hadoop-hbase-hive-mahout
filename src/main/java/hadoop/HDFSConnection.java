/**
 * Created by xugang on 2016/10/26.
 */

package hadoop;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS文件上传下载实例
 */
public class HDFSConnection {

    //hadoop fs的配置文件
    static  Configuration conf = new Configuration();

    /**
     * 将本地文件(localPath)上传到HDFS服务器的指定路径(hdfsPath)
     * @param localPath
     * @param hdfsPath
     * @throws Exception
     */
    public static void uploadFileToHDFS(String localPath,String hdfsPath) throws Exception {
        //创建一个文件系统
        FileSystem fs = FileSystem.get(conf);
        Path path1 = new Path(localPath);
        Path path2 = new Path(hdfsPath);
        Long start = System.currentTimeMillis();
        fs.copyFromLocalFile(false, path1, path2);
        System.out.println("Time:"+ (System.currentTimeMillis() - start));

        System.out.println("________________________Upload to "+conf.get("fs.default.name")+"________________________");
        fs.close();
        getDirectoryFromHdfs(hdfsPath);
    }
    /**
     * 下载文件
     * @param hdfsPath
     * @throws Exception
     */
    public static void downLoadFileFromHDFS(String hdfsPath) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        InputStream in = fs.open(path);
        try {
            //将文件COPY到标准输出(即控制台输出)
            IOUtils.copyBytes(in, System.out, 4096,false);
        }finally{
            IOUtils.closeStream(in);
            fs.close();
        }
    }
    /**
     * 遍历指定目录(direPath)下的所有文件
     * @param direPath
     * @throws Exception
     */
    public static void  getDirectoryFromHdfs(String direPath) throws Exception{

        FileSystem fs = FileSystem.get(URI.create(direPath),conf);
        FileStatus[] filelist = fs.listStatus(new Path(direPath));
        for (int i = 0; i < filelist.length; i++) {
            System.out.println("_________________***********************____________________");
            FileStatus fileStatus = filelist[i];
            System.out.println("Name:"+fileStatus.getPath().getName());
            System.out.println("size:"+fileStatus.getLen());
            System.out.println("_________________***********************____________________");
        }
        fs.close();
    }
    /**
     * 测试方法
     * @param args
     */
    public static void main(String[] args) {
        try {
            getDirectoryFromHdfs("/test");

            uploadFileToHDFS("C:/xugang/test/record.txt", "/test");
//            downLoadFileFromHDFS("/test/wc.txt");


        } catch (Exception e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
    }

}
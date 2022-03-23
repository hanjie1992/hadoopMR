package homework.chapter3.book;
/**
 *将本地D:\test.txt文件上传至HDFS的/mydir下
 */

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS Upload
 *
 */
public class SS_04_上传文件 {
    public static void main( String[] args ) throws Exception {
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.13.129:9000");
        //指定用户名,获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"hadoop");

        //构造一个输入流
        InputStream is = new FileInputStream("d:\\test4.txt");

        //得到一个输出流
        OutputStream os = fs.create(new Path("/mydir/test4.txt"));

        // 使用工具类实现复制
        IOUtils.copyBytes(is, os, 1024);

        //关闭流
        is.close();
        os.close();

        //不需要再操作FileSystem了，关闭client
        fs.close();

        System.out.println( "Upload Successfully!" );
    }
}
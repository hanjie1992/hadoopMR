package homework.chapter3.book;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS Download
 */
public class SS_08_下载文件
{
    public static void main( String[] args ) throws Exception
    {
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.13.129:9000");
        //指定用户名,获取FileSystem对象
        FileSystem client=FileSystem.get(uri,conf,"hadoop");

        //打开一个输入流 <------HDFS
        InputStream is = client.open(new Path("/mydir/test.txt"));

        //构造一个输出流  ----> d:\test2.txt
        OutputStream os = new FileOutputStream("d:\\test8.txt");

        // 使用工具类实现复制
        IOUtils.copyBytes(is, os, 1024);

        //关闭流
        is.close();
        os.close();

        //不需要再操作FileSystem了，关闭client
        client.close();

        System.out.println( "Download Successfully!" );
    }
}
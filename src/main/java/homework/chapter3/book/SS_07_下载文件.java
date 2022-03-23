package homework.chapter3.book;


/**
 *将HDFS上的/mydir/test.txt文件下载到本地/usr/b_copy
 */
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS Download
 */
public class SS_07_下载文件
{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.13.129:9000");

        //指定用户名,获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"hadoop");

        //HDFS file
        Path src=new Path("/mydir/test.txt");
        //local file
        Path dst=new Path("d:\\test7.txt");

        //Linux下
        //fs.copyToLocalFile(src,dst);

        //Windows下
        fs.copyToLocalFile(false,src,dst,true);

        //不需要再操作FileSystem了，关闭
        fs.close();

        System.out.println( "Download Successfully!" );
    }
}
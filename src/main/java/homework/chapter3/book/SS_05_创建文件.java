package homework.chapter3.book;

/**
 *在HDFS上创建/mydir/test2.txt文件
 */
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.URI;

/**
 * HDFS： Create File
 *
 */
public class SS_05_创建文件 {
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.13.129:9000");
        //指定用户名,获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"hadoop");
        //define new file
        Path dfs=new Path("/mydir/test5.txt");
        FSDataOutputStream os=fs.create(dfs,true);
        os.writeBytes("hello,hdfs!");

        //关闭流
        os.close();

        //不需要再操作FileSystem了，关闭
        fs.close();
    }
}
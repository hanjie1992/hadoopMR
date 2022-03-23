package homework.chapter3.book;

/**
 *删除HDFS上的/mydir/test2.txt文件
 */
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * HDFS Delete
 */
public class SS_09_删除文件{
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.13.129:9000");
        //指定用户名,获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"hadoop");

        //HDFS file
        Path path=new Path("/mydir/test2.txt");
        fs.delete(path);

        //不需要再操作FileSystem了，关闭
        fs.close();

        System.out.println( "Delete File Successfully!" );
    }
}
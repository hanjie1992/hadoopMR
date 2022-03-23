package homework.chapter3.book;
/**
 * 查看HDFS上的/mydir/test.txt文件的详细信息
 */

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS Meta
 *
 */
public class SS_06_查看文件详细信息
{
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.13.129:9000");
        //指定用户名,获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"hadoop");

        //指定路径
        Path path=new Path("/mydir/test.txt");

        //获取状态
        FileStatus fileStatus=fs.getFileLinkStatus(path);

        //获取数据块大小
        long blockSize=fileStatus.getBlockSize();
        System.out.println("blockSize:"+blockSize);

        //获取文件大小
        long fileSize=fileStatus.getLen();
        System.out.println("fileSize:"+fileSize);

        //获取文件拥有者
        String fileOwner=fileStatus.getOwner();
        System.out.println("fileOwner:"+fileOwner);

        //获取最近访问时间
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
        long accessTime=fileStatus.getAccessTime();
        System.out.println("accessTime:"+sdf.format(new Date(accessTime)));

        //获取最后修改时间
        long modifyTime=fileStatus.getModificationTime();
        System.out.println("modifyTime:"+sdf.format(new Date(modifyTime)));

        //不需要再操作FileSystem了，关闭
        fs.close();
    }
}
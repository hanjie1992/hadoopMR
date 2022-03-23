package homework.chapter3.book;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SS_02_Write {
//    会遇到权限问题，需要在hdfs-site.xml添加配置
/*    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
      </property>
*/

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.13.128:8020");
        FileSystem fs = FileSystem.get(uri,conf,"node");
        Path path = new Path("/mydir/test2.txt");
        byte[] buff = "hello world".getBytes();
        FSDataOutputStream dos = fs.create(path);
        dos.write(buff);
        dos.close();
        fs.close();
    }

}

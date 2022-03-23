package homework.chapter3.book;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class SS_01_Read {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.13.128:8020");
        FileSystem fs = FileSystem.get(uri,conf,"node");
        Path src = new Path("/mydir/test.txt");
        FSDataInputStream dis = fs.open(src);
        String str = null;
        while ((str=dis.readLine()) != null){
            System.out.println(str);
        }
        dis.close();
        fs.close();
    }
}

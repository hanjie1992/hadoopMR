package homework.chapter5.ss_02_localwordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("mapreduce.framework.name", "yarn");//集群的方式运行，非本地运行
        config.set("mapreduce.app-submission.cross-platform", "true");//意思是跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，去网上搜答案很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，但是其实添加了这行代码就可以了。
        config.set("mapreduce.job.jar","D:\\workspace\\hadoop\\target\\hadoop-1.0-SNAPSHOT.jar");

        //创建一个job和任务入口
        Job job = Job.getInstance(config);
        job.setJarByClass(WordCountMain.class);  //main方法所在的class
        //指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);   //k2的类型
        job.setMapOutputValueClass(IntWritable.class);  //v2的类型
        //指定job的reducer和输出的类型<k4  v4>
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);  //k4的类型
        job.setOutputValueClass(IntWritable.class);  //v4的类型
        //指定job的输入和输出
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileInputFormat.setInputPaths(job,"hdfs://192.168.13.128:8020/input/data.txt");
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.13.128:8020/output/ww"));

        //执行job
        job.waitForCompletion(true);
    }
}

package homework.chapter5.ss_05_writable;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SalaryTotalMain {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("mapreduce.framework.name", "yarn");//集群的方式运行，非本地运行
        config.set("mapreduce.app-submission.cross-platform", "true");//意思是跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，去网上搜答案很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，但是其实添加了这行代码就可以了。
        config.set("mapreduce.job.jar","D:\\workspace\\hadoop\\target\\hadoop-1.0-SNAPSHOT.jar");
        Job job = Job.getInstance(config);
        //  创建一个job0
//        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SalaryTotalMain.class);
        //指定job的mapper和输出的类型   k2  v2
        job.setMapperClass(SalaryTotalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Employee.class);
        //指定job的reducer和输出的类型  k4   v4
        job.setReducerClass(SalaryTotalReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //指定job的输入和输出的路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //判断输出目录是否存在，如果已经存在删除
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.13.128:8020"),config,"node");
        fs.delete(new Path("/output/salarywritable"),true);
        //指定job的输入和输出的路径
        FileInputFormat.setInputPaths(job,"hdfs://192.168.13.128:8020/input/salary.csv");
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.13.128:8020/output/salarywritable"));
        fs.close();

        //执行任务
        job.waitForCompletion(true);
    }
}

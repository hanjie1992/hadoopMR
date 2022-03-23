package homework.chapter5.ss_03_windowswordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String icIn = "D:\\workspace\\hadoop\\src\\main\\data\\data.txt";
        String out = "D:\\outfiledata\\worddata";

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS", "file:///");

        //1. 获取Job实例
//        Job job = Job.getInstance(new Configuration());
        Job job = Job.getInstance(conf);

        //2. 设置Jar包
        job.setJarByClass(WcDriver.class);

        //3. 设置Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //4. 设置Map和Reduce的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5. 设置输入输出文件
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Path inpath = new Path(icIn);
        Path outPath = new Path(out);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)){
            System.out.println("输出路径 " + outPath.getName() + " 已存在,默认删除");
            fs.delete(outPath, true);
        }
        FileInputFormat.setInputPaths(job, inpath);
        FileOutputFormat.setOutputPath(job, outPath);

        //6. 提交Job
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}

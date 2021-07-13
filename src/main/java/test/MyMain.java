
package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class MyMain {
	
	public static void main(String[] args) throws Exception{
		String icIn = "E:\\workspace\\hadoop\\src\\main\\data\\data-han.csv";
		String out = "G:\\house_data-han";

		Configuration conf  = new Configuration();
		conf.set("mapreduce.framework.name","local");
		conf.set("fs.defaultFS", "file:///");

		Job job=Job.getInstance(new Configuration());
		job.setJarByClass(MyMain.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//5. 设置输入输出文件
		Path inpath = new Path(icIn);
		Path outPath = new Path(out);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)){
			System.out.println("输出路径 " + outPath.getName() + " 已存在,默认删除");
			fs.delete(outPath, true);
		}
		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outPath);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
		
	}

}
package BookTopN.topN_order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class topNMain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String icIn = "E:\\workspace\\hadoop\\src\\main\\data\\order.txt";
		String out = "G:\\hadoop\\order_data";
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name","local");
		conf.set("fs.defaultFS", "file:///");

		Job job = Job.getInstance(conf);
		//Map
		job.setMapperClass(topNMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Order.class);
		
		//reduce
		job.setReducerClass(topNReducer.class);
		job.setOutputKeyClass(Order.class);
		job.setOutputValueClass(NullWritable.class);

		Path inpath = new Path(icIn);
		Path outPath = new Path(out);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)){
			System.out.println("输出路径 " + outPath.getName() + " 已存在,默认删除");
			fs.delete(outPath, true);
		}
		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outPath);
		
//		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:8020/dir"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/out01"));
		
		job.waitForCompletion(true);
	}

}

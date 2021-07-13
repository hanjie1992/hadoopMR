package student;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class GraduateMainM {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String icIn = "D:\\workspace\\hadoop\\src\\main\\data\\input\\";
		String out = "D:\\outfiledata\\worddata";
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name","local");
		conf.set("fs.defaultFS", "file:///");

	    Job job = Job.getInstance();
	    job.setJarByClass(GraduateMainM.class);
	    
	    job.setMapperClass(GraduateMapperM.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setReducerClass(GraduateReducerM.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);

		Path inpath = new Path(icIn);
		Path outPath = new Path(out);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)){
			System.out.println("输出路径 " + outPath.getName() + " 已存在,默认删除");
			fs.delete(outPath, true);
		}
		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

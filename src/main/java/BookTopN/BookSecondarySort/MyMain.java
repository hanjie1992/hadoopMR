


package BookTopN.BookSecondarySort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyMain {
	public static void main(String[] args) throws Exception {
		String icIn = "E:\\workspace\\hadoop\\src\\main\\data\\books.txt";
		String out = "G:\\hadoop\\books_data";
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name","local");
		conf.set("fs.defaultFS", "file:///");

		Job job = Job.getInstance(conf);
		
		//map
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Books.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//reduce函数没有，不进行相关设置
		//设置输入输出路径
		Path inpath = new Path(icIn);
		Path outPath = new Path(out);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)){
			System.out.println("输出路径 " + outPath.getName() + " 已存在,默认删除");
			fs.delete(outPath, true);
		}
		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outPath);

//		FileInputFormat.setInputPaths(job, new Path("hdfs://xdata-m1:8020/user/ua50/books.txt"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://xdata-m1:8020/user/ua50/chenteng/2019061402"));
		job.waitForCompletion(true);
	}
}

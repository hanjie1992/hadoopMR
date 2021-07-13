package student;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GraduateMapperM extends Mapper<LongWritable,Text,Text,IntWritable>{

	String file_name;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();
		file_name = fs.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] data = value.toString().split(",");
		if(file_name.contains("cd")) {
			if(data.length > 6 && !data[6].equals("工资待遇（必填）") ){
				if(!data[6].isEmpty()) {
					if(data[6].length()>4) {
						String[] data1 = data[6].split("-");
						int a = Integer.parseInt(data1[0]);
						int b = Integer.parseInt(data1[1]);
						int avg = (a + b)/2;
						context.write(new Text("承德薪资"), new IntWritable(avg));
					}else {
						int pay = Integer.parseInt(data[6]);
						context.write(new Text("承德薪资"), new IntWritable(pay));
					}
				}
			}
	    }else if(file_name.contains("xc")) {
			if(!data[13].equals("综合薪资")) {
				if(!data[13].isEmpty() && !data[13].contains("无")) {
					int pay1 = Integer.parseInt(data[13]);
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~============================================================================================================================");
					System.out.println(pay1);
					context.write(new Text("许昌薪资"), new IntWritable(pay1));
				}
			}
		}
	}
}

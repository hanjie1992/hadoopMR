package BookTopN.topN_order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class topNMapper extends Mapper<LongWritable, Text, Text, Order>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		String[] lines=value.toString().split(",");
		if (lines.length>1) {
			float price = Float.parseFloat(lines[3]);
			int number = Integer.parseInt(lines[4]);
			float sum = price*number;
			Order order = new Order(lines[0], lines[1], lines[2], price, number, sum);
			
			context.write(new Text(lines[1]), order);
		}
	}
}

package student;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraduateReducerM extends Reducer<Text,IntWritable,Text,DoubleWritable>{

	Map<String,String> map = new HashMap<String,String>();
	
	@Override
	protected void reduce(Text k3, Iterable<IntWritable> v3,
			Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		int total = 0;
		int count = 0;
		for(IntWritable v:v3) {
			total += v.get();
			count += 1;
		}
		String info = total + "-" + count;
		map.put(k3.toString(), info);
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
//			String[] info = map.get("承德薪资").split("-");
//			int cd_sum = Integer.parseInt(info[0]);
//			int cd_count = Integer.parseInt(info[1]);
			System.out.println("===========================================================================================================================================================================================");
		    System.out.println(map.get("许昌薪资"));
			String[] info2 = map.get("许昌薪资").split("-");
			int xc_sum = Integer.parseInt(info2[0]);
			int xc_count = Integer.parseInt(info2[1]);
			
//			double cd_avg = new BigDecimal((float)(cd_sum/cd_count))
//					.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
			double xc_avg = new BigDecimal((float)(xc_sum/xc_count))
					.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			
//			context.write(new Text("承德平均薪资："), new DoubleWritable(cd_avg));
			context.write(new Text("许昌平均薪资："), new DoubleWritable(xc_avg));
	}

	
}

package test;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MyReducer extends Reducer <Text,IntWritable,Text,IntWritable> {

	@Override
	protected void reduce(Text k3, Iterable<IntWritable> v3,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		int total = 0;
		int count=0;
		for(IntWritable v:v3) {
			total += v.get();
			count++;
			
		}
		int avg=total/count;
		context.write(k3, new IntWritable(avg));
	}
	

}



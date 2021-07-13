package BookTopN.topN_MR;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	Map<String, Integer> map=new HashMap<String,Integer>();
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		int count=0;
		for (IntWritable wc : arg1) {
			count+=wc.get();
		}
		String name = arg0.toString();
		map.put(name, count);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.cleanup(context);
		List<Entry<String, Integer>> list=new LinkedList<Entry<String,Integer>>(map.entrySet());
		Collections.sort(list,new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				// TODO Auto-generated method stub
				return (int)(o2.getValue()-o1.getValue());
			}
		});
		for(int i=0;i<3;i++){
			context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
		}
	}

}

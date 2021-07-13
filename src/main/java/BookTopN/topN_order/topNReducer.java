package BookTopN.topN_order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class topNReducer extends Reducer<Text, Order, Order, NullWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<Order> arg1, Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		ArrayList<Order> list = new ArrayList<Order>();
		for (Order order : arg1) {
			Order o=new Order(order.getOrderId(),order.getUserId(),order.getProduct(),order.getPrice(),order.getNumber(),order.getSum());
			list.add(o);
		}
		Collections.sort(list);
		for(int i=0;i<3;i++) {
			arg2.write(list.get(i), NullWritable.get());
		}
	}
}

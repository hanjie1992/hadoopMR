package BookTopN.BookSecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Books,NullWritable>{
//	注意：只有实现二次排序的Books对象在写出的key的位置才会执行自己的排序规则，在value写出时不进行排序,
//	要实现book的二次排序，必须把Books放在key的位置
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		//新建书对象
		Books books = new Books();
		String[] lines = line.split("\t");
		books.setReaderId(lines[0]);
		books.setBookId(lines[1]);
		books.setBookName(lines[2]);
		books.setBorrowDate(lines[3]);
		books.setBackDate(lines[4]);
		books.setAddress(lines[8]);
		
		context.write(books,NullWritable.get());
	}
}

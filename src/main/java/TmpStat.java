import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TmpStat
{
    public static class StatMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private IntWritable intValue = new IntWritable();
        private Text dateKey = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String[] items = value.toString().split(",");

            String date = items[0];
            String tmp = items[5];

            if(!"DATE".equals(date) && !"N/A".equals(tmp))
            {//排除第一行说明以及未取到数据的行
                dateKey.set(date.substring(0, 6));
                intValue.set(Integer.parseInt(tmp));
                context.write(dateKey, intValue);
            }
        }
    }

    public static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int tmp_sum = 0;
            int count = 0;

            for(IntWritable val : values)
            {
                tmp_sum += val.get();
                count++;
            }

            int tmp_avg = tmp_sum/count;
            result.set(tmp_avg);
            context.write(key, result);
        }
    }

    public static void main(String args[])
            throws IOException, ClassNotFoundException, InterruptedException
    {
//        String icIn = "G:\\beijing.txt";
        String icIn = "E:\\workspace\\hadoop\\src\\main\\data\\beijing.txt";
        String out = "G:\\data";

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);
        job.setJarByClass(TmpStat.class);
        job.setMapperClass(StatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(StatReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
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
package lianjia;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LianJiaData{
    public static class LianJiaMapper extends Mapper<LongWritable, Text,Text,Text>{
        public void map(LongWritable key,Text value,Mapper<LongWritable, Text,Text,Text>.Context context) throws IOException,InterruptedException{
            String data = value.toString();
            String[] info = data.split(",");
            if(info.length > 10){
                if(!"area".equals(info[1])){
                    String area = info[1].trim();
                    double size = Double.valueOf(info[2].trim());
                    int total_price = Integer.valueOf(info[7].trim());
                    context.write(new Text(area),new Text(size + "-" +total_price));
                }
            }
        }
    }
    public static class LianJiaReduce extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> value, Reducer<Text,Text,Text,Text>.Context context)
                throws IOException,InterruptedException{
            int sum = 0;
            int total = 0;
            for(Text v:value){
                String info = v.toString();
                String[] values = info.split("-");
                double size = Double.valueOf(values[0]);
                int total_price = Integer.valueOf(values[1]);
                sum += size;
                total += total_price;
            }
            int price = total*10000/sum;
            context.write(key,new Text(price+"元/平方米"));
        }


    }
    public static void main(String args[]) throws Exception{
        String icIn = "E:\\workspace\\hadoop\\src\\main\\data\\house_data.txt";
        String out = "G:\\house_data";

        Configuration conf  = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS", "file:///");
        //1. 获取Job实例
        Job job = Job.getInstance(conf);
        //2. 设置Jar包
        job.setJarByClass(LianJiaData.class);
        //3. 设置Mapper和Reducer
        job.setMapperClass(LianJiaMapper.class);
        job.setReducerClass(LianJiaReduce.class);
        //4. 设置Map和Reduce的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //5. 设置输入输出文件
        Path inpath = new Path(icIn);
        Path outPath = new Path(out);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)){
            System.out.println("输出路径 " + outPath.getName() + " 已存在,默认删除");
            fs.delete(outPath, true);
        }
        FileInputFormat.setInputPaths(job, inpath);
        FileOutputFormat.setOutputPath(job, outPath);
        //6. 提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}

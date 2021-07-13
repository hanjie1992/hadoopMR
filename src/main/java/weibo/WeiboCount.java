package weibo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeiboCount extends Configured implements Tool {
    // tab分隔符
    private static String TAB_SEPARATOR = "\t";
    // 粉丝
    private static String FAN = "fan";
    // 关注
    private static String FOLLOWERS = "followers";
    // 微博数
    private static String MICROBLOGS = "microblogs";

    public static class WeiBoMapper extends Mapper<Text, WeiBo, Text, Text> {
        @Override
        protected void map(Text key, WeiBo value, Context context) throws IOException, InterruptedException {
            // 粉丝
            context.write(new Text(FAN), new Text(key.toString() + TAB_SEPARATOR + value.getFan()));
            // 关注
            context.write(new Text(FOLLOWERS), new Text(key.toString() + TAB_SEPARATOR + value.getFollowers()));
            // 微博数
            context.write(new Text(MICROBLOGS), new Text(key.toString() + TAB_SEPARATOR + value.getMicroblogs()));
        }
    }

    public static class WeiBoReducer extends Reducer<Text, Text, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> mos;

        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, IntWritable>(context);
        }

        protected void reduce(Text Key, Iterable<Text> Values,Context context) throws IOException, InterruptedException {
            Map<String,Integer> map = new HashMap< String,Integer>();

            for(Text value : Values){ //增强型for循环，意思是把Values的值传给Text value
                // value = 名称 + (粉丝数 或 关注数 或 微博数)
                String[] records = value.toString().split(TAB_SEPARATOR);
                map.put(records[0], Integer.parseInt(records[1].toString()));
            }

            // 对Map内的数据进行排序
            Map.Entry<String, Integer>[] entries = getSortedHashtableByValue(map);
            for(int i = 0; i < entries.length;i++){
                mos.write(Key.toString(),entries[i].getKey(), entries[i].getValue());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();  // 配置文件对象

        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS", "file:///");

        // 判断路径是否存在，如果存在，则删除
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }


        Job job = new Job(conf, "weibo");  // 构造任务

        job.setJarByClass(WeiboCount.class); // 主类

        // Mapper
        job.setMapperClass(WeiBoMapper.class);
        // Mapper key输出类型
        job.setMapOutputKeyClass(Text.class);
        // Mapper value输出类型
        job.setMapOutputValueClass(Text.class);

        // Reducer
        job.setReducerClass(WeiBoReducer.class);
        // Reducer key输出类型
        job.setOutputKeyClass(Text.class);
        // Reducer value输出类型
        job.setOutputValueClass(IntWritable.class);

        // 输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 自定义输入格式
        job.setInputFormatClass(WeiboInputFormat.class) ;
        //自定义文件输出类别
        MultipleOutputs.addNamedOutput(job, FAN, TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, FOLLOWERS, TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, MICROBLOGS, TextOutputFormat.class, Text.class, IntWritable.class);

        // 去掉job设置outputFormatClass，改为通过LazyOutputFormat设置
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        //提交任务
        return job.waitForCompletion(true)?0:1;
    }

    // 对Map内的数据进行排序（只适合小数据量）
    @SuppressWarnings("unchecked")
    public static Entry<String, Integer>[] getSortedHashtableByValue(Map<String, Integer> h) {
        Entry<String, Integer>[] entries = (Entry<String, Integer>[]) h.entrySet().toArray(new Entry[0]);
        // 排序
        Arrays.sort(entries, new Comparator<Entry<String, Integer>>() {
            public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
                return entry2.getValue().compareTo(entry1.getValue());
            }
        });
        return entries;
    }

    public static void main(String[] args) throws Exception {
        String icIn = "E:\\workspace\\hadoop\\src\\main\\data\\weibo.txt";
        String out = "G:\\webdata";
        String[] args0 = {icIn, out};

//      String[] args0 = { "./data/weibo/weibo.txt", "./out/weibo/" };
        int ec = ToolRunner.run(new Configuration(), new WeiboCount(), args0);
        System.exit(ec);
    }
}

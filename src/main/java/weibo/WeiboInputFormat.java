package weibo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


/*
 * 其实这个程序，就是在实现InputFormat接口，TVPlayInputFormat是InputFormat接口的实现类
 * 比如   WeiboInputFormat  extends FileInputFormat implements InputFormat。
 * 问：自定义输入格式 WeiboInputFormat 类，首先继承 FileInputFormat，然后分别
 * 重写 isSplitable()方法和 createRecordReader() 方法。
 * 线路是： boolean isSplitable()  -> RecordReader<Text,WeiBo> createRecordReader()
 *          -> WeiboRecordReader extends RecordReader<Text, WeiBo >
 */
public class WeiboInputFormat extends FileInputFormat<Text,WeiBo>{

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        /*
        * 这是InputFormat的isSplitable方法。
        * isSplitable方法就是是否要切分文件，这个方法显示如果是压缩文件就不切分，非压缩文件就切分。
        * 如果不允许分割，则isSplitable==false，则将第一个block、文件目录、开始位置为0，长度
        * 为整个文件的长度封装到一个InputSplit，加入splits中。如果文件长度不为0且支持分割，
        * 则isSplitable==true,获取block大小，默认是64MB
        */
        return false;    //整个文件封装到一个InputSplit
        //要么就是return true; 切分64MB大小的一块一块，再封装到InputSplit
    }

    @Override
    public RecordReader<Text, WeiBo> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        /*
        * RecordReader<k1, v1>是返回类型,返回的RecordReader对象的封装
        * createRecordReader是方法,在这里是，WeiboInputFormat.createRecordReader。
        * WeiboInputFormat是InputFormat类的实例。
        * InputSplit input和TaskAttemptContext context是传入参数
        *
        * isSplitable(),如果是压缩文件就不切分，整个文件封装到一个InputSplit。
        * isSplitable(),如果是非压缩文件就切，切分64MB大小的一块一块，再封装到InputSplit。
        *
        * 这里默认是系统实现的的RecordReader，按行读取，下面我们自定义这个类WeiboRecordReader。
        * 类似与Excel、WeiBo、TVPlayData代码写法。
        *
        * 自定义WeiboRecordReader类，按行读取
        */
        return new WeiboRecordReader();
        //新建一个ScoreRecordReader实例，所有才有了上面RecordReader<Text,ScoreWritable>，所以才如下ScoreRecordReader，写我们自己的
    }



    public class WeiboRecordReader extends RecordReader<Text, WeiBo>{
        /*
        * LineReader      in是1，行号。
        * Text line;      俞灏明    俞灏明    10591367    206    558，每行的相关记录
        */
        public LineReader in;  //行读取器
        public Text line;//每行数据类型
        public Text lineKey;//自定义key类型，即k1
        public WeiBo lineValue;//自定义value类型，即v1

        @Override
        public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException { //初始化，都是模板

            FileSplit split = (FileSplit)input;  // 获取split

            Configuration job = context.getConfiguration(); // 获取配置

            Path file = split.getPath();  // 分片路径

            FileSystem fs = file.getFileSystem(job);

            FSDataInputStream filein = fs.open(file);   // 打开文件

            in=new LineReader(filein,job); //输入流in
            line=new Text();//每行数据类型
            lineKey=new Text();//自定义key类型，即k1。//新建一个Text实例作为自定义格式输入的key
            lineValue = new WeiBo();//自定义value类型，即v1。//新建一个TVPlayData实例作为自定义格式输入的value
        }


        //此方法读取每行数据，完成自定义的key和value
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException { //这里面，才是篡改的重点
            // 一行数据
            Text line = new Text();
            int linesize = in.readLine(line);
            /*
            * line是每行数据，我们这里用到的是in.readLine(str)这个构造函数，默认读完读到文件末尾。其实这里有三种。
            * 是SplitLineReader.readLine  ->  SplitLineReader extends LineReader -> org.apache.hadoop.util.LineReader
            * in.readLine(str)//这个构造方法执行时，会首先将value原来的值清空。默认读完读到文件末尾
            * in.readLine(str, maxLineLength)//只读到maxLineLength行
            * in.readLine(str, maxLineLength, maxBytesToConsume)//这个构造方法来实现不清空，前面读取的行的值
            */
            if(linesize == 0)
                return false;

            // 通过分隔符'\t'，将每行的数据解析成数组 pieces
            String[] pieces = line.toString().split("\t");//因为，我们这里是。默认读完读到文件末尾。line是Text类型。pieces是String[]，即String数组。

            if(pieces.length != 5){
                throw new IOException("Invalid record received");
            }

            int a,b,c;
            try{

                a = Integer.parseInt(pieces[2].trim()); //粉丝数,//将String类型，如pieces[2]转换成，float类型，给a

                b = Integer.parseInt(pieces[3].trim()); //关注数

                c = Integer.parseInt(pieces[4].trim()); //微博数
            }catch(NumberFormatException nfe){
                throw new IOException("Error parsing floating poing value in record");
            }

            //自定义key和value值
            lineKey.set(pieces[0]);  //完成自定义key数据
            lineValue.set(a, b, c); //完成自定义value数据
//            或者写
//            lineValue.set(Integer.parseInt(pieces[2].trim()),Integer.parseInt(pieces[3].trim()),Integer.parseInt(pieces[4].trim()));
            return true;
        }

        @Override
        public void close() throws IOException { //关闭输入流
            if(in != null){
                in.close();
            }
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException { //获取当前的key,即CurrentKey
            return lineKey; //返回类型是Text,即Text lineKey
        }

        @Override
        public WeiBo getCurrentValue() throws IOException, InterruptedException { //获取当前的Value，即CurrentValue
            return lineValue; //返回类型是WeiBo,即WeiBo lineValue
        }

        @Override
        public float getProgress() throws IOException, InterruptedException { //获取进程，即Progress
            return 0; //返回类型是float,即float 0
        }

    }
}

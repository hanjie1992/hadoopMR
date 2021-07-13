package weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/*
数据格式：    明星     明星微博名称                 粉丝数     关注数     微博数
            黄晓明    黄晓明                        22616497    506    2011
            张靓颖    张靓颖                        27878708    238    3846
            张成龙2012    张成龙2012                9813621        199    744
            羅志祥    羅志祥                        30763518    277    3843
            劉嘉玲    劉嘉玲                        12631697    350    2057
            吳君如大美女    吳君如大美女        18490338    190    412
            柯震東Kai    柯震東Kai                31337479    219    795
            李娜    李娜                        23309493    81    631
            徐小平    徐小平                        11659926    1929    13795
            唐嫣    唐嫣                        24301532    200    2391
            有斐君    有斐君                        8779383    577    4251
            孙燕姿    孙燕姿                        21213839    68    342
            成龙    成龙                        22485765    5    758
*/

public class WeiBo implements WritableComparable<Object> {
    /*
    * 其实这里，跟TVPlayData和ScoreWritable一样的
    * 注意：    Hadoop通过Writable接口实现的序列化机制，不过没有提供比较功能，
    * 所以和java中的Comparable接口合并，提供一个接口WritableComparable。（自定义比较）
    *
    * Writable接口提供两个方法(write和readFields)。
    */
    //  直接利用java的基本数据类型int，定义成员变量fan、followers、microblogs
    // 粉丝
    private int fan;
    // 关注
    private int followers;
    // 微博数
    private int microblogs;

//    问：这里我们自己编程时，是一定要创建一个带有参的构造方法，为什么还要显式的写出来一个带无参的构造方法呢？
//    答：构造器其实就是构造对象实例的方法，无参数的构造方法是默认的，但是如果你创造了一个带有参数的构造方法，那么无参的构造方法必须显式的写出来，否则会编译失败。

    public WeiBo(){}; //java里的无参构造函数，是用来在创建对象时初始化对象
    //在hadoop的每个自定义类型代码里，好比，现在的WeiBo，都必须要写无参构造函数。


    //问：为什么我们在编程的时候，需要创建一个带有参的构造方法？
    //答:就是能让赋值更灵活。构造一般就是初始化数值，你不想别人用你这个类的时候每次实例化都能用另一个构造动态初始化一些信息么(当然没有需要额外赋值就用默认的)。
    public WeiBo(int fan,int followers,int microblogs){
        this.fan = fan;
        this.followers = followers;
        this.microblogs = microblogs;
    }


    //问：其实set和get方法，这两个方法只是类中的setxxx和getxxx方法的总称，
    //那么，为什么在编程时，有set和set***两个，只有get***一个呢？
    public void set(int fan,int followers,int microblogs){
        this.fan = fan;
        this.followers = followers;
        this.microblogs = microblogs;
    }

//    public float get(int fan,int followers,int microblogs){因为这是错误的，所以对于set可以分开，get只能是get***
//    return fan;
//    return followers;
//    return microblogs;
//}

    /*
    * 实现WritableComparable的readFields()方法，以便该数据能被序列化后完成网络传输或文件输入
    * 对象不能传输的，需要转化成字节流！
    * 将对象转换为字节流并写入到输出流out中是序列化，write 的过程（最好记!!!）
    * 从输入流in中读取字节流反序列化为对象      是反序列化，readFields的过程（最好记!!!）
    */
    @Override
    public void readFields(DataInput in) throws IOException {
        //拿代码来说的话，对象就是比如fan、followers。。。。
        fan  = in.readInt(); //因为，我们这里的对象是Int类型，所以是readInt()
        followers = in.readInt();
        microblogs = in.readInt(); //注意:反序列化里，需要生成对象对吧，所以，是用到的是get对象
//        in.readByte()
//        in.readChar()
//        in.readDouble()
//        in.readLine()
//        in.readFloat()
//        in.readLong()
//        in.readShort()
    }
    /*
    * 实现WritableComparable的write()方法，以便该数据能被序列化后完成网络传输或文件输出。
    * 将对象转换为字节流并写入到输出流out中是序列化，write 的过程（最好记!!!）
    * 从输入流in中读取字节流反序列化为对象      是反序列化，readFields的过程（最好记!!!）
    */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fan);
        out.writeInt(followers); //因为，我们这里的对象是Int类型，所以是writeInt()
        out.writeInt(microblogs); //注意:序列化里，需要对象对吧，所以，用到的是set那边的对象
//        out.writeByte()
//        out.writeChar()
//        out.writeDouble()
//        out.writeFloat()
//        out.writeLong()
//        out.writeShort()
//        out.writeUTF()
    }

    @Override
    public int compareTo(Object o) { //java里的比较，Java String.compareTo()
        // TODO Auto-generated method stub
        return 0;
    }
    /*
    * Hadoop中定义了两个序列化相关的接口：Writable 接口和 Comparable 接口，这两个接口可以
    * 合并成一个接口 WritableComparable。 Writable 接口中定义了两个方法，分别
    * 为write(DataOutput out)和readFields(DataInput in)。所有实现了Comparable接口
    * 的对象都可以和自身相同类型的对象比较大小
    *
    */

//  源码是
//    package java.lang;
//    import java.util.*;
//    public interface Comparable {
//        /**
//        * 将this对象和对象o进行比较，约定：返回负数为小于，零为大于，整数为大于
//        */
//        public int compareTo(T o);
//    }

    public int getFan() {
        return fan;
    }

    public void setFan(int fan) {
        this.fan = fan;
    }

    public int getFollowers() {
        return followers;
    }

    public void setFollowers(int followers) {
        this.followers = followers;
    }

    public int getMicroblogs() {
        return microblogs;
    }

    public void setMicroblogs(int microblogs) {
        this.microblogs = microblogs;
    }
}
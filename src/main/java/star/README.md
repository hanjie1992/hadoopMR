

项目我们使用明星搜索指数数据，分别统计出搜索指数最高的男明星和女明星。

思路分析

1、编写 Mapper类，按需求将数据集解析为 key=gender，value=name+hotIndex，然后输出。

2、编写 Combiner 类，合并 Mapper 输出结果，然后输出给 Reducer。

3、编写 Partitioner 类，按性别，将结果指定给不同的 Reduce 执行。

4、编写 Reducer 类，分别统计出男、女明星的最高搜索指数。

5、编写 run 方法执行 MapReduce 任务

MapReduce Java 项目

设计的MapReduce如下所示：
Map = {key = gender, value = name+hotIndex}
Reduce = {key = name, value = gender+hotIndex}

Map
每次调用map(LongWritable key, Text value, Context context)解析一行数据。每行数据存储在value参数值中。然后根据'\t'分隔符，解析出明星姓名，性别和搜索指数。
map()函数期望的输出结果Map = {key = gender, value = name+hotIndex}

Combiner
对 map 端的输出结果，先进行一次合并，减少数据的网络输出。

Partitioner
根据明星性别对数据进行分区，将 Mapper 的输出结果均匀分布在 reduce 上。

Reduce
调用reduce(key, Iterable< Text> values, context)方法来处理每个key和values的集合。我们在values集合中，计算出明星的最大搜索指数
reduce()函数期望的输出结果Reduce = {key = name, value = gender+max(hotIndex)}

Run 驱动方法
在 run 方法中,设置任务执行各种信息。
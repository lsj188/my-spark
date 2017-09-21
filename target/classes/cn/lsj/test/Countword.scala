package cn.lsj.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/11/12.
 */
object Countword {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Wordcount")
    //==================优化参数============================
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  //序列化类
    // 注册自定义类型，默认只支持常用类型，如有比较复杂的类型及自定义类型需要注册
//    conf.set("spark.kryo.registrator", "com.ultrapower.noas.javaspark.tools.Registorator");
    //决定了能够传输消息的最大值（默认为10M），单位M，当应用里有需要传输大数据量的时候可以调整此参数
    conf.set("spark.akka.frameSize", "1000");
    //Spark内存缓存的堆大小占用总内存比例（默认0.6），不能大于JVM Old区内存大小
    conf.set("spark.storage.memoryFraction", "0.4");
    conf.set("spark.shuffle.manager", "sort"); // 采用sort shuffle，有两种可用的实现：sort和hash
    // 只对sort起效，当reduce分区小于多少的时候在SortShuffleManager内部不使用Merge Sort的方式处理数据，
    // 而是与Hash Shuffle类似，去除Sort步骤来加快处理速度，代价是需要并发打开多个文件，
    // 所以内存消耗量增加；当GC严重时可减小此值
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "100");
    conf.set("spark.shuffle.compress", "true"); // 是否压缩map操作的输出文件。
    conf.set("spark.shuffle.spill.compress", "true"); // 在shuffle时，是否将spilling的数据压缩。压缩算法通过spark.io.compression.codec指定。
    conf.set("spark.rdd.compress", "true"); // 是否压缩序列化的RDD分区。在花费一些额外的CPU时间的同时节省大量的空间
    conf.set("spark.default.parallelism", "100") //shuffle过程默认使用的task数量（默认为8）,如果是sql的话，可修改spark.sql.shuffle.partitions
    //==================优化参数============================

    val sc = new SparkContext(conf)
    //    val data = sc.textFile("E:\\bigdata\\spark-2.2.0-bin-hadoop2.7\\README.md") // 文本存放的位置
    val data = sc.textFile("hdfs://localhost:9000/lsj_test/README.txt") // hdfs
    data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    Thread.sleep(20000)
  }
}
package cn.lsj.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/11/12.
  */
object Countword {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Wordcount")
    val sc = new SparkContext(conf)
//    val data = sc.textFile("E:\\bigdata\\spark-2.2.0-bin-hadoop2.7\\README.md") // 文本存放的位置
    val data = sc.textFile("hdfs://localhost:9000/lsj_test/README.txt") // hdfs
    data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    Thread.sleep(20000)
  }
}
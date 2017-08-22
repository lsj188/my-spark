package cn.lsj.sqlapi

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lsj on 2017/8/22.
 */
object test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //将一个RDD隐式转换为DataFrame
    import sqlContext.implicits._
    case class dim_date(time_day: Int, year: Int, month: String, day: String, week: Int, quarter: Int)

    //读取文件转为mapRDD，将数据注入dim_date隐式转换为DataFrame
    val dim_dateDF = sc.textFile("E:\\git\\my-spark\\testData\\mytest.t_dim_date.txt").map(_.split(",")).map(cols =>
      dim_date(cols(0).toInt, cols(1).toInt, cols(2), cols(3), cols(4).toInt, cols(5).toInt)
    ).toDF()

    //注册为临时表
    dim_dateDF.registerTempTable("dim_date")

    //执行sql
    val dates = sqlContext.sql("select time_day,year,month,day,week,quarter from dim_date where quarter=1")
    dates.collect().foreach(println)
  }
}

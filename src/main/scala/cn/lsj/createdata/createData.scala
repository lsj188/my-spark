package cn.lsj.createdata

import java.io.IOException

import cn.lsj.tools.{DateTools, FileTools}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lsj on 2017/9/20.
 */
object createData {

  def main(args: Array[String]) {
    table2txt()
  }

  def table2txt(): Unit = {
    val outPutFile="E:\\git\\my-spark\\testData\\test.courses"
    val (sc, sqlContext) = initSpark("local", "createData")
    val startTime="2017-01-01 00:00:00"

    //读取mysql表数据
    val tabDF = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "courses")
      .option("user", "root")
      .option("password", "lsj123")
      .load()
    tabDF.cache()
    tabDF.show(10)
    delFilePath(outPutFile)
    // 将DataFrame转为Rdd
    for (i <- 0 to 1000)
    {
      val typeNum=i % 10
      val typeNum1=i % 50
      val tabRDD = tabDF.rdd.flatMap((row: Row) => for (col1 <- row.getString(0).split(","))
      yield List(col1+typeNum1,
          row.getString(1)+typeNum,
          row.getString(2)+i,
          row.getString(3)+i,
          row.getString(4)+i,
          row.getString(5)+i,
          row.getString(6),
          row.getString(7),
          DateTools.dayCalculate(startTime,i).substring(0,10)
          //        row.getTimestamp(8)
        ).mkString(","))

      tabRDD.saveAsTextFile(outPutFile+"\\in_time="+DateTools.dayCalculate(startTime,i).substring(0,10))
    }

    sc.stop()
  }


  /**
   * 初使化spark
   **/
  def initSpark(masterUrl: String, appName: String): (SparkContext, SQLContext) = {
    //    val conf = new SparkConf().setMaster("yarn-client").setAppName("Test")
    val conf = new SparkConf().setMaster(masterUrl).setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

  /**
   * 删除存在的目录
   **/
  def delFilePath(path: String): Unit = {
    // 删除目标路径
    try {
      FileTools.delPath(path)
    } catch {
      case ex: IOException => ex.printStackTrace()
    }
  }
}

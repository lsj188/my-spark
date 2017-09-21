package cn.lsj

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

    //读取mysql表数据
    val tabDF = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "courses")
      .option("user", "root")
      .option("password", "lsj123")
      .load()
    tabDF.show(10)

    // 将DataFrame转为Rdd
    val tabDF1 = tabDF.rdd.flatMap((row: Row) => for (col1 <- row.getString(0).split(","))
    yield List(col1,
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getString(7),
        DateTools.Date2formate(row.getTimestamp(8))
//        row.getTimestamp(8)
      ).mkString(","))
    delFilePath(outPutFile)
    tabDF1.saveAsTextFile(outPutFile)
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

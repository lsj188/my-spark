package cn.lsj.sqlapi

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lsj on 2017/8/22.
 */
object demo {
  val hadoopdsPath = "hdfs://localhost:9000"

  def main(args: Array[String]) {

    //    sql1 //使用反射机制推断RDD Schema
    //    sql2 //以编程方式定义RDD Schema。
    demofile2parquet() //文本文件转为parquet文件
    readParquet()      //读取parquet文件
  }

  /**
   * Parquet文件数据源
   * 场景：适用于spark-sql底层文件，或hive on spark的hive表底层文件
   * 特点：列存储存，更少的IO和存储空间（平均可压缩75%左右）；
   * Parquet读取器使用了下推过滤器来进一步减少磁盘 IO。
   * 下推式过滤器允许在将数据读入 Spark 之前就制定数据选择决策。
   * 平面文件会读取表中所有行和列
   **/
  def readParquet(): Unit = {
    //获取SparkContext, SQLContext

    val (sc, sqlContext) = initSpark()
    val fileName = hadoopdsPath + "/lsj_test/parquet/dim_date"
    val parquetDF = sqlContext.read.parquet(fileName)
    parquetDF.createOrReplaceTempView("parquetFile")
    val namesDF = sqlContext.sql("SELECT time_day FROM parquetFile WHERE quarter =1")
    //    namesDF.map(attributes => "Name: " + attributes(0)).show()
    namesDF.show()
    sc.stop()
  }

  /**
   * text to parquet
   * Spark SQL内置数据源短名称有json、parquet、jdbc，默认parquet（通过“spark.sql.sources.default”配置）
   **/
  def demofile2parquet(): Unit = {
    //获取SparkContext, SQLContext
    val (sc, sqlContext) = initSpark()

    //定义schema Array("time_day", "year", "month", "day", "week", "quarter")
    val schema = StructType(Array(
      StructField("time_day", IntegerType, false),
      StructField("year", IntegerType, false),
      StructField("month", StringType, false),
      StructField("day", StringType, false),
      StructField("week", IntegerType, false),
      StructField("quarter", IntegerType, false)))


    val inPath = hadoopdsPath + "/lsj_test/"
    val outPath = hadoopdsPath + "/lsj_test/parquet/"
    val tabName = "dim_date"
    text2parquet(sqlContext, inPath, outPath, schema, tabName)

    sc.stop()
  }

  /**
   * 将文本文件转为parquet文件
   * sqlContext
   * inPath
   * outPath
   * schema
   * tablename
   **/
  def text2parquet(sqlContext: SQLContext, inPath: String, outPath: String, schema: StructType, tablename: String) {
    // import text-based table first into a data frame
    //    val df = sqlContext.read.format("com.databricks.spark.csv").schema(schema).option("delimiter", "|").load(inPath + tablename + "/*")
    val df = sqlContext.read.format("com.databricks.spark.csv").schema(schema).option("delimiter", ",").load(inPath + tablename + "/*")
    // now simply write to a parquet file
    //    df.show(20)

    df.write.mode(SaveMode.Overwrite).format("parquet").option("delimiter","|").save(outPath + tablename)
    df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("delimiter","|").save("E:\\tmp\\" + tablename)
//    df.write.parquet(outPath + tablename)
  }

  /**
   * 时间维度
   **/
  case class dim_date(time_day: Int, year: Int, month: String, day: String, week: Int, quarter: Int)

  /**
   * 方法：使用反射机制推断RDD Schema
   * 场景：运行前知道Schema
   * 特点：代码简洁
   * 问题：Error:(22, 7) value toDF is not a member of org.apache.spark.rdd.RDD[dim_date]
   * possible cause: maybe a semicolon is missing before value toDF'?
   * 1. import sqlContext.implicits._ 语句需要放在获取sqlContext对象的语句之后
   * 2. case class dim_date 的定义需要放在方法的作用域之外（即成员变量位置）
   *
   **/
  def sql1: Unit = {
    //获取SparkContext, SQLContext
    val (sc, sqlContext) = initSpark()

    //将一个RDD隐式转换为DataFrame
    import sqlContext.implicits._
    //读取文件转为MapperRDD，将数据注入dim_date隐式转换为DataFrame
    val dim_dateDF = sc.textFile("E:\\git\\my-spark\\testData\\mytest.t_dim_date.txt").map(_.split(",")).map(cols =>
      dim_date(cols(0).toInt, cols(1).toInt, cols(2), cols(3), cols(4).toInt, cols(5).toInt)
    ).toDF()

    //注册为临时表
    dim_dateDF.registerTempTable("dim_date")

    //执行sql
    val dates = sqlContext.sql("select time_day,year,month,day,week,quarter from dim_date where quarter=1")
    dates.collect().foreach(println)

    sc.stop()
  }

  /**
   * 方法：以编程方式定义RDD Schema。
   * 场景：运行前不知道Schema。
   * 特点：
   **/
  def sql2: Unit = {

    //获取SparkContext, SQLContext
    val (sc, sqlContext) = initSpark()

    //读取文件转为MapperRDD
    val fileRdd = sc.textFile("E:\\git\\my-spark\\testData\\mytest.t_dim_date.txt")

    //运行时从别处获取schema
    val schemaArray = Array("time_day", "year", "month", "day", "week", "quarter")

    //创建schema
    val schema = StructType(schemaArray.map(col => StructField(col, StringType, true)))

    //将文本转为Rdd
    val rowRdd = fileRdd.map(_.split(",")).map(cols => Row(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5)))

    //将schema应用于RDD
    val dim_dateDF = sqlContext.createDataFrame(rowRdd, schema)

    //注册为临时表
    dim_dateDF.registerTempTable("dim_date")

    //执行sql
    val dates = sqlContext.sql("select time_day,year,month,day,week,quarter from dim_date where quarter=1")
    //    dates.collect().foreach(println)

    //将数据以parquet文件格式写入
    dates.write.parquet("E:\\git\\my-spark\\testData\\parquet\\mytest.t_dim_date.parquet")
    sc.stop()
  }

  /**
   * 初使化spark
   **/
  def initSpark(): (SparkContext, SQLContext) = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

}

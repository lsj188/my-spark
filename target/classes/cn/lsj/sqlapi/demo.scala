package cn.lsj.sqlapi

import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lsj on 2017/8/22.
 * http://spark.apache.org/docs/latest/sql-programming-guide.html#loading-data-programmatically
 */
object demo {
  val hadoopdsPath = "hdfs://localhost:9000"

  def main(args: Array[String]) {

    //    sql1 //使用反射机制推断RDD Schema
    //    sql2 //以编程方式定义RDD Schema。
    //    demofile2parquet() //文本文件转为parquet文件
    //    readParquet() //读取parquet文件
    //    partParquet() //分区表
    //    hivesql()     //执行hive sql
    //    useJson()
//    useFunctions()
    useJDBC()
  }

  /**
   * 连接jdbc数据源
   **/
  def useJDBC(): Unit = {
    val (sc, sqlContext) = initSpark()
    val parquetDF=sqlContext.read.parquet("E:\\git\\my-spark\\spark-warehouse\\saveastab_test/")
    val jdbcConnectionProperties=new Properties()
    jdbcConnectionProperties.put("user","root")
    jdbcConnectionProperties.put("password","lsj123")
    parquetDF.show()

    //写入mysql数据库表，当mode为SaveMode.OverWrite时，会先删表再创建表
    parquetDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/test","saveastab_test",jdbcConnectionProperties)

    //读取关系数据库表
    val jdbcDF = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "saveastab_test")
      .option("user","root")
      .option("password","lsj123")
      .load()
    jdbcDF.show()
  }

  /**
   * 函数使用
   *
   **/
  def useFunctions(): Unit = {
    val (sc, sqlContext) = initSpark()
    val parquetDF = sqlContext.read.parquet("e:/git/my-spark/spark-warehouse/lsj_test.db/t_dim_date_p")
    parquetDF.createTempView("test_dim_time")
    val tabDF = sqlContext.sql("select * from test_dim_time")
    println("tabDF=", tabDF.count())

    //选择列
    val joinDF = tabDF.join(parquetDF, Seq("date"), "inner").select(tabDF.col("date"), parquetDF.col("month"))
    joinDF.show(10)

    //聚合
    joinDF.groupBy(joinDF.col("month")).agg(sum("date").as("sum_date"), count("month").as("cnt"), avg("date").as("date_avg")).show(12)

    //字段计算
    //    joinDF.withColumn("date1",joinDF.col("date")+1).withColumn("date+month",joinDF.col("date")+joinDF.col("month")).show(10)


    //开窗函数的使用，分析函org.apache.spark.sql.functions包中可查看支持哪些函数
    //    joinDF.withColumn("rn",row_number().over(Window.partitionBy("month").orderBy("date"))).filter("rn=1").show(100)
    val joinDF1 = joinDF.withColumn("month_sum_date", sum("date").over(Window.partitionBy("month"))).
      withColumn("rn", row_number().over(Window.partitionBy("month").orderBy("date"))).filter("rn=1")
//    joinDF1.show(100)

    //将DataFrame存为表，可分区，分桶
    joinDF1.coalesce(1).write.mode(SaveMode.Overwrite).bucketBy(4, "date").partitionBy("month").saveAsTable("saveastab_test")

    sqlContext.sql("select * from saveastab_test").show(100)

    sc.stop()


  }

  /**
   * json
   **/
  def useJson(): Unit = {
    val infileName = hadoopdsPath + "/lsj_test/parquet/dim_date"
    val outfileName = "E:\\tmp\\dim_date.json"
    val (sc, sqlContext) = initSpark()
    val parquetDF = sqlContext.read.parquet(infileName)
    parquetDF.write.mode(SaveMode.Append).json(outfileName)
    val jsonDF = sqlContext.read.json(outfileName)
    jsonDF.createTempView("json_dim_date")
    val resultDF = sqlContext.sql("select count(*) as cnt from json_dim_date")
    resultDF.show()
    sc.stop()
  }

  /**
   * HiveContext
   * 操作Hive数据源须创建SQLContext的子类HiveContext对象。
   * Standalone集群：添加hive-site.xml到$SPARK_HOME/conf目录。
   * YARN集群：添加hive-site.xml到$YARN_CONF_DIR目录；添加Hive元数据库JDBC驱动jar文件到$HADOOP_HOME/lib目录。
   * 最简单方法：通过spark-submit命令参数--file和--jar参数分别指定hive-site.xml和Hive元数据库JDBC驱动jar文件。
   * 未找到hive-site.xml：当前目录下自动创建metastore_db和warehouse目录。
   **/

  def hivesql(): Unit = {
    //获取SparkContext, SQLContext
    val (sc, sqlContext) = initSpark()
    val hiveContext = new HiveContext(sc)
    //val sql="CREATE TABLE lsj_test.t_dim_date_p\n(\n  date                   BIGINT      ,\n  year                   int         ,\n  month                  int         ,\n  day                    int         ,\n  week                   int         ,\n  quarter                int\n)\nROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' stored as\nINPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
    //hiveContext.sql(sql)

    val fileName = hadoopdsPath + "/lsj_test/parquet/dim_date"
    //    val parquetDF = sqlContext.read.parquet(fileName)
    //    parquetDF.createOrReplaceTempView("parquetFile")
    //    hiveContext.sql("insert overwrite table lsj_test.t_dim_date_p select * from parquetFile")
    //    hiveContext.sql("create table lsj_test.t_dim_date_p1 as select * from lsj_test.t_dim_date_p")
    //    val htab=hiveContext.sql("select * from lsj_test.t_dim_date_p1")
    //    htab.show(100)
    //    htab.write.parquet("")
    val parquetDF = hiveContext.read.parquet("e:/git/my-spark/spark-warehouse/lsj_test.db/t_dim_date_p")
    parquetDF.createTempView("test_dim_time")
    val tabDF = sqlContext.sql("select * from test_dim_time")
    tabDF.show(10)
    println("tabDF=", tabDF.count())
    val joinCnt = tabDF.join(parquetDF, Seq("date"), "inner").select(tabDF.col("date"), parquetDF.col("month")) //选择列

    sc.stop()
  }

  /**
   * parquet分区
   **/
  def partParquet(): Unit = {
    //获取SparkContext, SQLContext
    val (sc, sqlContext) = initSpark()
    val fileName = hadoopdsPath + "/lsj_test/parquet/dim_date"
    val parquetDF = sqlContext.read.parquet(fileName)
    parquetDF.createOrReplaceTempView("parquetFile")
    val part1 = sqlContext.sql("select time_day, year, month, day, week from parquetFile where quarter =1")
    part1.write.mode(SaveMode.Overwrite).parquet(hadoopdsPath + "/lsj_test/parquet/dim_date_part/quarter=1")
    val part2 = sqlContext.sql("select time_day, year, month, day, week from parquetFile where quarter =2")
    part2.write.mode(SaveMode.Overwrite).parquet(hadoopdsPath + "/lsj_test/parquet/dim_date_part/quarter=2")
    val part3 = sqlContext.sql("select time_day, year, month, day, week from parquetFile where quarter =3")
    part3.write.mode(SaveMode.Overwrite).parquet(hadoopdsPath + "/lsj_test/parquet/dim_date_part/quarter=3")
    val part4 = sqlContext.sql("select time_day, year, month, day, week from parquetFile where quarter =4")
    part4.write.mode(SaveMode.Overwrite).parquet(hadoopdsPath + "/lsj_test/parquet/dim_date_part/quarter=4")
    //////////////////////////////////////////////////////
    val partparquetDF = sqlContext.read.option("mergeSchema", "true").parquet(hadoopdsPath + "/lsj_test/parquet/dim_date_part")
    parquetDF.show()
    parquetDF.printSchema()
    sc.stop()

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
    txt2parquet(sqlContext, inPath, outPath, schema, tabName)

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
  def txt2parquet(sqlContext: SQLContext, inPath: String, outPath: String, schema: StructType, tablename: String) {
    // import text-based table first into a data frame
    //    val df = sqlContext.read.format("com.databricks.spark.csv").schema(schema).option("delimiter", "|").load(inPath + tablename + "/*")
    val df = sqlContext.read.format("com.databricks.spark.csv").schema(schema).option("delimiter", ",").load(inPath + tablename + "/*")
    // now simply write to a parquet file
    //    df.show(20)

    df.write.mode(SaveMode.Overwrite).format("parquet").option("delimiter", "|").save(outPath + tablename)

    //保存为txt文件
    //    df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("delimiter","|").save("E:\\tmp\\" + tablename)
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
    //    val conf = new SparkConf().setMaster("yarn-client").setAppName("Test")
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

}

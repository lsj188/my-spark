package cn.lsj.rdd

import java.io.IOException

import cn.lsj.tools.FileTools
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Main {
    def main(args: Array[String]): Unit = {
        //    if (args(0) == "CoursesSum") {
        //      val cour: CoursesSum = new CoursesSum()
        //      cour.run(args)
        //    }
        val cour = new CoursesSum()
        cour.run()
    }
}

/**
 * Created by lsj on 2017/9/19.
 */
object init {

    /**
     * 初使化spark
     **/
    def initSpark(masterUrl: String, appName: String): (SparkContext, SQLContext) = {
        //    val conf = new SparkConf().setMaster("yarn-client").setAppName("Test")
        val conf = new SparkConf().setMaster(masterUrl).setAppName(appName)
        //==================优化参数，根据业务及数据量大小调整============================
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //序列化类
        // 注册自定义类型，默认只支持常用类型，如有比较复杂的类型及自定义类型需要注册
        //    conf.set("spark.kryo.registrator", "com.ultrapower.noas.javaspark.tools.Registorator");
        //决定了能够传输消息的最大值（默认为10M），单位M，当应用里有需要传输大数据量的时候可以调整此参数
        conf.set("spark.akka.frameSize", "1000")
        //Spark内存缓存的堆大小占用总内存比例（默认0.6），不能大于JVM Old区内存大小
        conf.set("spark.storage.memoryFraction", "0.4")
        conf.set("spark.shuffle.manager", "sort") // 采用sort shuffle，有两种可用的实现：sort和hash
        // 只对sort起效，当reduce分区小于多少的时候在SortShuffleManager内部不使用Merge Sort的方式处理数据，
        // 而是与Hash Shuffle类似，去除Sort步骤来加快处理速度，代价是需要并发打开多个文件，
        // 所以内存消耗量增加；当GC严重时可减小此值
        conf.set("spark.shuffle.sort.bypassMergeThreshold", "100")
        conf.set("spark.shuffle.compress", "true") // 是否压缩map操作的输出文件。
        conf.set("spark.shuffle.spill.compress", "true") // 在shuffle时，是否将spilling的数据压缩。压缩算法通过spark.io.compression.codec指定。
        conf.set("spark.rdd.compress", "true") // 是否压缩序列化的RDD分区。在花费一些额外的CPU时间的同时节省大量的空间
        //    conf.set("spark.default.parallelism", "100") //shuffle过程默认使用的task数量（可以简单的理解为reduce个数，默认为8）,如果是sql的话，可修改spark.sql.shuffle.partitions
        //==================优化参数============================
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        (sc, sqlContext)
    }
}

class CoursesSum() extends Serializable {
    /**
     * | Field        | Type          | Null | Key | Default | Extra |
     * +--------------+---------------+------+-----+---------+-------+
     * | type         | varchar(100)  | YES  |     | NULL    |       |
     * | level        | varchar(20)   | YES  |     | NULL    |       |
     * | title        | varchar(200)  | YES  |     | NULL    |       |
     * | url          | varchar(300)  | YES  |     | NULL    |       |
     * | image_path   | varchar(300)  | YES  |     | NULL    |       |
     * | image_url    | varchar(300)  | YES  |     | NULL    |       |
     * | student      | varchar(10)   | YES  |     | NULL    |       |
     * | introduction | varchar(3000) | YES  |     | NULL    |       |
     * | in_time      | datetime      | YES  |     | NULL    |       |
+--------------+---------------+------+-----+---------+-------+
     **/
    def run(): Unit = {
        val inFiles = List("E:\\git\\my-spark\\testData\\test.courses")
        val outFiles = List("E:\\git\\my-spark\\testData\\test.courses.sum1",
            "E:\\git\\my-spark\\testData\\test.courses.sum2",
            "E:\\git\\my-spark\\testData\\test.courses.join1",
            "E:\\git\\my-spark\\testData\\test.courses.leftjoin",
            "E:\\git\\my-spark\\testData\\test.courses.rightjoin"
        )
        val (sc, sqlContext) = init.initSpark("local", "coureseSum")
        // 删除目标路径
        try {
            for (path <- outFiles)
                FileTools.delPath(path)
        } catch {
            case ex: IOException => ex.printStackTrace()
        }

        val coureseRdd = sc.textFile(inFiles(0)).map(_.split(","))
        //缓存RDD数据
        coureseRdd.persist(StorageLevel.MEMORY_AND_DISK)
        //按类型和级别统计人数
        val students = coureseRdd
          .map((cols: Array[String]) => ((cols(0), cols(1)), cols(6).toLong))
          .reduceByKey(_ + _)
          .coalesce(1) //缩减文件数
          .sortBy((x) => (x._1._1, x._2), false) //降序排序
          .map((s) => s._1._1 + "," + s._1._2 + "," + s._2) //处理文件输出格式
        students.take(20).foreach(println) //取前20条数据打印
        students.saveAsTextFile(outFiles(0))

        //按级别统计人数及课程门数
        val levelStudents = coureseRdd
          .map((cols) => ((cols(1), cols(3), cols(6).toLong), 1)) //处理为K，V（(level,url,student),1），方便去重
          .reduceByKey((a, b) => a) //数据去重
          .map((row) => (row._1._1, (row._1._3, row._2))) //重新构建K，V()
          .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
          .map((row) => List(row._1, row._2._1, row._2._2).mkString(",")) // 处理文件输入格式
        levelStudents.saveAsTextFile(outFiles(1))

        val leftRdd = students.map((line) => {
            val cols = line.split(",")
            (cols(1), cols)
        }) //处理成可关联的K，V
        val rightRdd = levelStudents.union(sc.parallelize(Seq(List("未知", "123", "456").mkString(","), List("未知1", "1231", "4561").mkString(",")))).map((line) => {
                val cols = line.split(",")
                (cols(0), cols)
            }) //处理成可关联的K，V
        val innerjoinRdd = leftRdd
              .join(rightRdd) //处理成可关联的K，V
              .map((obj) => obj._2._1.mkString(",") + "," + obj._2._2.mkString(",")) //拼接关联结果
        innerjoinRdd.saveAsTextFile(outFiles(2))

        val leftJoinRdd = leftRdd.leftOuterJoin(rightRdd).map((obj) => obj._2._1.mkString(",") + "," + (obj._2._2 match {
            case Some(a) => a.mkString(",")
            case None => "No this Skill"
        })).coalesce(1).saveAsTextFile(outFiles(3))
        val rightJoinRdd = leftRdd.rightOuterJoin(rightRdd).map((obj) => (obj._2._1 match {
            case Some(a) => a.mkString(",")
            case None => "No this Skill"
        }) + "," + obj._2._2.mkString(",")).coalesce(1).saveAsTextFile(outFiles(4))





        Thread.sleep(10000000)

    }


}

//class IeteFlowSum() {
//
//}

package cn.lsj.kafkaAndStream

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


object WordCount {
    def main(args: Array[String]) {

        val conf = new SparkConf()
        conf.setAppName("spark_streaming")
        conf.setMaster("local[*]")

        val sc = new SparkContext(conf)
        sc.setCheckpointDir("D:/checkpoints")
        sc.setLogLevel("ERROR")

        val ssc = new StreamingContext(sc, Seconds(5))

        val topics = Map("spark" -> 2)
        val lines = KafkaUtils.createStream(ssc, "localhost:2181", "spark", topics).map(_._2)

        val ds1 = lines.flatMap(_.split(" ")).map((_, 1))

        val ds2 = ds1.updateStateByKey[Int]((x:Seq[Int], y:Option[Int]) => {
            Some(x.sum + y.getOrElse(0))
        })

        ds2.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
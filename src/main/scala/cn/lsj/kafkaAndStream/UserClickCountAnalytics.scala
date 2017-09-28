package cn.lsj.kafkaAndStream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap


object UserClickCountAnalytics {

    def main(args: Array[String]): Unit = {
        var masterUrl = "local[1]"
        if (args.length > 0) {
            masterUrl = args(0)
        }

        // Create a StreamingContext with the given master URL
        val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
        val ssc = new StreamingContext(conf, Seconds(5))

        // Kafka configurations
        val topics = Set("user_events")
        val brokers = "127.0.0.1:9092"
        val groupid = "consumer_group1"
        val kafkaParams = HashMap[String, String]("metadata.broker.list" -> brokers,
            "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> groupid)

        val dbIndex = 1
        val clickHashKey = "app::users::click"

        // Create a direct stream
        //        val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        //        val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        val kafkaManager = new KafkaManager(kafkaParams)
        val kafkaStream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        val events = kafkaStream.transform((rdd) => {
            //更新数据标记
            kafkaManager.updateZKOffsets(rdd); rdd
        }
        ).flatMap(line => {
            val data = new JSONObject(line._2)
            Some(data)
        })

        // Compute user click times
        //        val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_+_)
        val userClicks = events.map(x => (x.getString("uid"), (x.getInt("click_count"), 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        userClicks.foreachRDD(rdd => {
            rdd.foreachPartition(partitionOfRecords => {
                val jedis = RedisClient.pool.getResource
                //选择数据库
                //                    jedis.select(dbIndex)
                jedis.select(2)
                partitionOfRecords.foreach(pair => {
                    val uid = pair._1
                    val map1 = jedis.hgetAll(uid)
                    val clickCount = pair._2._1 + (
                      if (map1.get("clickCount") != null) map1.get("clickCount") else "0"
                      ).toInt
                    val rowCount = pair._2._2 + (
                      if (map1.get("rowCount") != null) map1.get("rowCount") else "0"
                      ).toInt

                    val map = Map("clickCount" -> clickCount.toString, "rowCount" -> rowCount.toString)

                    //                    jedis.hincrBy(clickHashKey, uid, clickCount)
                    jedis.hmset(uid, mapAsJavaMap(map)) //由于hmset参数需要JAVA的Map类型所以需要转
                    //                    println("uid=",uid,"clickCount=",clickCount)
                })
                RedisClient.pool.returnResource(jedis)
            })
        })

        ssc.start()
        ssc.awaitTermination()

    }
}
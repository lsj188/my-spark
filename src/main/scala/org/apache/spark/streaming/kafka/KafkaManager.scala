package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

/**
 * 增读取数据标记位，使客户端读取数据不丢失
 **/
class KafkaManager(val kafkaParams: HashMap[String, String]) extends Serializable {

    private val kc = new KafkaCluster(kafkaParams)

    /**
     * 创建数据流
     *
     * @param ssc
     * @param kafkaParams
     * @param topics
     * @tparam K
     * @tparam V
     * @tparam KD
     * @tparam VD
     * @return
     */
    def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K] : ClassTag,
    VD <: Decoder[V] : ClassTag]
    (
      ssc: StreamingContext,
      kafkaParams: HashMap[String, String],
      topics: Set[String]
      ): InputDStream[(K, V)] = {
        val groupId = kafkaParams.get("group.id").get
        val partitionsE = kc.getPartitions(topics)


        if (partitionsE.isLeft)
            throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
        val partitions = partitionsE.right.get
        val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
        if (!consumerOffsetsE.isLeft) {
            val consumerOffsets = consumerOffsetsE.right.get
            KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
                ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
        } else {
            val p = kafkaParams + ("auto.offset.reset" -> "largest")
            KafkaUtils.createDirectStream(ssc, p, topics)
        }
        //    }
        //      messages
    }


    /**
     * 更新zookeeper上的消费offsets
     *
     * @param rdd
     */
    def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
        val groupId = kafkaParams.get("group.id").get
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        for (offsets <- offsetsList) {
            val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
            val o = kc.setConsumerOffsets(groupId, HashMap((topicAndPartition, offsets.untilOffset)))
            if (o.isLeft) {
                println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
            }
        }
    }
}
package utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

/**
 * Offset管理工具类，用于往Redis中存取和读取offset
 *
 * 管理方案：
 * 1、后置提交偏移量 -》 手动控制偏移量提交
 * 2、手动控制偏移量提交 -》 SparkStreaming提供了手动提交方案，但是我们不能用，因为我们会对DStream的结构进行转换
 * 3、手动的提取偏移量维护到Redis中
 * -》 从Kafka中消费到暑假，先提取偏移量
 * -》 等数据成功写出后，将偏移量存储到Redis中
 * -》 从Kafka中消费数据之前，先到Redis中读取偏移量，使用读取到的偏移量到Kafka中消费数据
 * 4、手动的将偏移量存储到Redis中，每次消费数据需要使用存储的Offset进行消费，每次消费数据后，要将本次消费的Offset存储到Redis中
 */
object MyOffsetsUtils {

  /**
   * 往Redis中存储Offset
   * 问题：存的Offset从哪来？
   * 从消费到的数据中提取出来的，传入到该方法中。
   * offsetRanges: Array[OffsetRange]
   * Offset的结构是什么？
   * Kafka中Offset维护的结构
   * groupId + topic + partition =》 offset
   * 从传入进入的Offset中提取关键信息
   * 在Redis中怎么存？
   * 类型：string，list，set，zset，（hash）
   * key：topic + groupId
   * value：partition - offset，partition - offset。。。
   * 写入API：hset/hmset
   * 读取API：hgetall
   * 是否过期：不过期
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.length > 0) {
      val offsets: util.HashMap[String, String] = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition = offsetRange.partition
        val endOffset: Long = offsetRange.untilOffset
        offsets.put(partition.toString, endOffset.toString)
      }
      println("提交offset：" + offsets)
      //往Redis中存
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()
      val redisKey: String = s"offsets:$topic:$groupId"
      jedis.hset(redisKey, offsets)
      jedis.close()
    }
  }

  /**
   * 往Redis中读取存储的Offset
   *
   * 问题：
   * 如何让SparkStreaming通过指定的offset进行消费？
   *
   * SparkStreaming要求的offset都格式是什么？
   * Map[TopicPartition,Long]
   */
  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisKey: String = s"offsets:$topic:$groupId"
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    println("读取到offset：" + offsets)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    //将Java的map装换成scala都map进行迭代
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsets.asScala) {
      val tp = new TopicPartition(topic, partition.toInt)
      results.put(tp, offset.toLong)
    }
    jedis.close()
    results.toMap
  }

}

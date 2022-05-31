package app

import bean.PageLog
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 *
 * 1、准备实时环境
 * 2、从Redis中读取偏移量
 * 3、从kafka中消费数据
 * 4、提取偏移量结束点
 * 5、处理数据
 * 5.1、转换数据结构
 * 5.2、去重
 * 5.3、维度关联
 * 6、写入ES
 * 7、提交offsets
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //1、准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dad_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //2、从Redis中读取offset
    val topicName: String = "DWD_PAGE_LOG_TOPIC_0522"
    val groupId: String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3、从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //4、提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5、处理数据
    //5.1转换结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    //5.2去重
    //  自我审查：将页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    //  第三方审查：通过redis将当日活跃的mid维护起来，自我审查后的每条数据需要到redis中进行比对去重
    // redis中如何维护日活状态？？？
    // 类型：String、（list）、（set）、zset、hash
    // key：DAU:DATE(0522)
    // value：mid的集合
    // 写入API：lpush/rpush、sadd
    // 读取API：lrange、smembers
    // 过期：24个小时过期
    //filterDStream.filter() 每条数据处理一次，redis连接太频繁
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        //存储要的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter) {
          //提取每条数据中的mid（我们日活的统计基于mid，也可以基于uid）
          val mid: String = pageLog.mid

          //获取日期
          val ts: Long = pageLog.ts
          val date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisKey: String = s"DAU:${dateStr}"

          //redis判断是否包含操作
          //下面代码在分布式环境中，存在并发问题，可能多个并行度同时进入到if中，导致最终pageLogs保留多条同一个mid的数据
          /*
          val setMids: util.Set[String] = jedis.smembers(redisKey)
          if(!setMids.contains(mid)){  多个并行度同时进入判断，都成功进入if
            jedis.sadd(redisKey,mid)   redis中虽然只会保留一次，但是pageLogs中会添加多次
            pageLogs.append(pageLog)
          }
          */

          val isNew: Long = jedis.sadd(redisKey, mid) // 判断包含和写入，实现了原子操作
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }

        }
        jedis.close()
        pageLogs.iterator
      }
    )
    redisFilterDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

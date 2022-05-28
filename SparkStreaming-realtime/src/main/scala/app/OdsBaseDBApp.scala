package app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}

/**
 * 业务数据消费分流
 *
 * 1、准备实时环境
 *
 * 2、从redis中读取偏移量
 *
 * 3、从kafka中消费数据
 *
 * 4、提取偏移量结束点
 *
 * 5、数据处理
 * 5.1、转换数据结构
 * 5.2、分流
 * 事实数据 =》 kafka
 * 维度数据 =》 kafka
 *
 * 6、flush kafka的缓冲区
 *
 * 7、提交数据
 */
object OdsBaseDBApp {
  def main(args: Array[String]): Unit = {
    //1、准备实时环境
    val conf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topicName: String = "ODS_BASE_DB_0522"
    val groupId: String = "ODS_BASE_DB_GROUP_0522"

    //2、从redis读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3、从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //4、提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5、处理数据
    //  5.1、转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value()
        val jSONObject: JSONObject = JSON.parseObject(dataJson)
        jSONObject
      }
    )

    //  5.2、分流

    //TODO 如何动态配置表清单
    //事实表清单
    val factTables: Array[String] = Array[String]("order_info", "order_detail" /*缺啥补啥*/)
    //维度表清单
    val dimTables: Array[String] = Array[String]("user_info", "base_province" /*缺啥补啥*/)

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //提取操作类型
              val operType: String = jsonObj.getString("type")
              val opValue: String = operType match {
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              //判断操作类型：1、明确什么操作。2、过滤不感兴趣的数据
              if (opValue != null) {
                //提取表名
                val tableName: String = jsonObj.getString("table")
                if (factTables.contains(tableName)) {
                  //事实数据
                  //提取数据
                  val data: String = jsonObj.getString("data")
                  // DWD_ORDER_INFO_I  DWD_ORDER_INFO_U  DWD_ORDER_INFO_D
                  val dwdTopicName: String = s"DWD_${tableName.toUpperCase}_${opValue}_0522"
                  MyKafkaUtils.send(dwdTopicName, data)
                }
                if (dimTables.contains(tableName)) {
                  //维度数据
                  //类型：（string），list，set，zset，hash
                  //        hash：整个表存成一个hash。要考虑目前数据量大小和将来数据量增长问题及高频访问问题
                  //        hash：一条数据一个hash。
                  //        string：一条数据存成一个jsonString
                  //key：DIM:表名:ID
                  //value：整条数据的jsonString
                  //读取API：get
                  //写入API：set
                  //过期：不过期

                  //提取数据中的id
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey: String = s"DIM:${tableName.toUpperCase}:${id}"
                  val jedis: Jedis = MyRedisUtils.getJedisFromPool()
                  jedis.set(redisKey, dataObj.toJSONString)
                  jedis.close()
                }
              }
            }
            //刷新kafka缓冲区
            MyKafkaUtils.flush()
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}

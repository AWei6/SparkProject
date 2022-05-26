package app

import bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaUtils, MyOffsetsUtils}

import java.lang

/**
 * 日志数据的消费分流
 *
 * 1、准备实时处理环境StreamingContext
 *
 * 2、从Kafka中消费数据
 *
 * 3、处理数据
 *    3.1、转换数据结构
 *      专用结构 自定义Bean
 *      通用结构 Map JsonObject
 *    3.2、分流
 *
 * 4、写出到DWD层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1、准备实时环境
    //TODO 注意并行度与Kafka中topic的分区个数的对应关系
    val conf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //2、从kafka中消费数据
    val topicName: String = "ODS_BASE_LOG_0522" // 对应生成器配置中的主题名
    val groupId: String = "ODS_BASE_LOG_GROUP_0522"

    //从Redis中读取offset，指定offset进行消费
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)
    if (offsets != null && offsets.nonEmpty) {
      //指定offset进行消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      //默认offset进行消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }


    //补充：从当前消费到的数据中提取Offsets，不对流中的数据做任何处理
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //在Driver中执行
        rdd
      }
    )

    //3、处理数据
    //3.1、转换数据结构
    val jsonDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        //获取ConsumerRecord中的value，value就是日志数据
        val log: String = consumerRecord.value()
        //转换成Json对象
        val jsonObject: JSONObject = JSON.parseObject(log)
        //返回
        jsonObject
      }
    )

    //3.2、分流
    //  日志数据：
    //    页面访问数据：
    //      公共字段
    //      页面数据
    //      曝光数据
    //      事件数据
    //      错误数据
    //    启动数据：
    //      公共字段
    //      启动数据
    //      错误数据

    val DWD_PAGE_LOG_TOPIC : String = "DWD_PAGE_LOG_TOPIC_0522" //页面访问
    val DWD_PAGE_DISPLAY_TOPIC : String = "DWD_PAGE_DISPLAY_TOPIC_0522" // 页面曝光
    val DWD_PAGE_ACTION_TOPIC : String = "DWD_PAGE_ACTION_TOPIC_0522" // 页面事件
    val DWD_START_LOG_TOPIC : String = "DWD_START_LOG_TOPIC_0522" // 启动数据
    val DWD_ERROR_LOG_TOPIC : String = "DWD_ERROR_LOG_TOPIC_0522" // 错误数据

    // 分流规则：
    //  错误数据：不做任何拆分，只要包含错误字段，直接整条数据发送到对应的topic
    //  页面数据：拆分成页面访问，曝光，事件 分别发送到对应的topic
    //  启动数据：发送到对应的topic

    jsonDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //分流过程
              //分流错误数据
              val errObject: JSONObject = jsonObj.getJSONObject("err")
              if (errObject != null) {
                //将错误数据发送到DWD_ERROR_LOG_TOPIC
                val errStr: String = jsonObj.toJSONString
                MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, errStr)
              } else {
                //提取公共字段
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                val ar: String = commonObj.getString("ar") //地区
                val uid: String = commonObj.getString("uid") // 用户ID
                val os: String = commonObj.getString("os") // 操作系统
                val ch: String = commonObj.getString("ch") // 渠道
                val isNew: String = commonObj.getString("is_new") // 是否新用户
                val md: String = commonObj.getString("md") // 手机型号
                val mid: String = commonObj.getString("mid") // 手机id
                val vc: String = commonObj.getString("vc") // 版本
                val ba: String = commonObj.getString("ba") // 手机品牌

                //提取时间戳
                val ts: Long = jsonObj.getLong("ts")
                //页面数据
                val pageObj: JSONObject = jsonObj.getJSONObject("page")
                if (pageObj != null) {
                  //提取page字段
                  val pageId: String = pageObj.getString("page_id")
                  val pageItem: String = pageObj.getString("item")
                  val pageItemType: String = pageObj.getString("item_type")
                  val duringTime: Long = pageObj.getLong("during_time")
                  val lastPageId: String = pageObj.getString("last_page_id")
                  val sourceType: String = pageObj.getString("source_type")

                  //封装成pageLog
                  val pageLog: PageLog = new PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, sourceType, duringTime, ts)
                  //发送到DWD_PAGE_LOG_TOPIC
                  val str: String = JSON.toJSONString(pageLog, new SerializeConfig(true))
                  MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, str)

                  //提取曝光数据
                  val displayJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                  if (displayJsonArr != null && displayJsonArr.size() > 0) {
                    for (i <- 0 until displayJsonArr.size()) {
                      //循环拿到每个曝光数据
                      val displayObj: JSONObject = displayJsonArr.getJSONObject(i)
                      //提取曝光字段
                      val displayType: String = displayObj.getString("display_type")
                      val displayItem: String = displayObj.getString("item")
                      val displayItemType: String = displayObj.getString("item_type")
                      val posId: String = displayObj.getString("pos_id")
                      val order: String = displayObj.getString("order")
                      //封装成PageDisplayLog
                      val pageDisplayLog: PageDisplayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, sourceType, duringTime, displayType, displayItem, displayItemType, order, posId, ts)
                      //写到DWD_PAGE_DISPLAY_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                    }
                  }
                  //提取事件数据
                  val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                  if (actionJsonArr != null && actionJsonArr.size() > 0) {
                    for (i <- 0 until actionJsonArr.size()) {
                      val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                      val actionId: String = actionObj.getString("action_id")
                      val actionItem: String = actionObj.getString("item")
                      val actionItemType: String = actionObj.getString("item_type")
                      val actionTs: lang.Long = actionObj.getLong("ts")
                      val pageActionLog: PageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, sourceType, duringTime, actionId, actionItem, actionItemType, actionTs, ts)
                      MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                    }
                  }
                }
                //启动数据
                val startObj: JSONObject = jsonObj.getJSONObject("start")
                if (startObj != null) {
                  val entry: String = startObj.getString("entry")
                  val openAdId: String = startObj.getString("open_ad_id")
                  val loadingTime: lang.Long = startObj.getLong("loading_time")
                  val openAdMs: lang.Long = startObj.getLong("open_ad_ms")
                  val openAdSkipMs: lang.Long = startObj.getLong("open_ad_skip_ms")
                  val startLog: StartLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                  MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
                }
              }
            }
            //foreachPartition里面：Executor端执行，每批次每分区执行一次
            //刷写Kafka
            MyKafkaUtils.flush()
          }
        )

        /*
        rdd.foreach(
          jsonObj => {
            // foreach里面：提交offset？？？ executor执行，每条数据执行一次，这里不行
            // foreach里面：刷写kafka缓冲区？？？ executor执行，每条数据执行一次。相当于同步发送消息
          }
        )
        */

        //foreachRDD里面，foreach外面：提交offset？？？Driver端执行，一批次执行一次（周期性）
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
        //foreachRDD里面，foreach外面：刷写到kafka缓冲区？？？Driver端执行，一批次执行一次（周期性），这里不行，分流是在executor端执行的，driver端做刷写，刷的不是同一个对象的缓存区
      }
    )
    //foreachRDD外面：提交offset？？？ Driver执行，每次启动程序执行一次，这里不行
    //foreachRDD外面：刷写kafka缓存区？？？ Driver执行，每次启动程序执行一次，这里不行
    ssc.start()
    ssc.awaitTermination()
  }
}

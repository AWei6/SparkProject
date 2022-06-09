package utils

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import scala.collection.mutable.ListBuffer

/**
 * ES工具类，用于对ES进行读写操作
 */
object MyEsUtils {
  /** 客户端对象 * */
  val esClient: RestHighLevelClient = build()

  /** 创建ES客户端对象 * */
  def build(): RestHighLevelClient = {
    val host: String = MyPropsUtils(MyConfig.ES_HOST)
    val port: String = MyPropsUtils(MyConfig.ES_PORT)
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host, port.toInt))
    val client = new RestHighLevelClient(restClientBuilder)
    client
  }

  /** 关闭ES对象 * */
  def close(): Unit = {
    if (esClient != null) esClient.close()
  }

  /**
   * 1、批写
   * 2、幂等写
   *
   * @param indexName
   */
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]): Unit = {
    val bulkRequest = new BulkRequest(indexName)
    for ((docId, docObj) <- docs) {
      val indexRequest = new IndexRequest()
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      indexRequest.source(dataJson, XContentType.JSON)
      indexRequest.id(docId)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   *
   */
  def searchField(indexName: String, fieldName: String): List[String] = {
    //先判断索引是否存在
    val getIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean = esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!isExists) return null
    //正常从ES中提取指定的字段
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest = new SearchRequest(indexName)
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName, null).size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceAsMap: util.Map[String, AnyRef] = hit.getSourceAsMap
      val mid: String = sourceAsMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }

  def main(args: Array[String]): Unit = {
    println(searchField("gmall_dau_info_0522_2022-05-22", "mid"))
  }
}

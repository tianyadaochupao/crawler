package com.tang.crawler.flink

import java.net.{InetAddress, InetSocketAddress}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.tang.crawler.flink.ShopFlink.Shop
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.elasticsearch.client.Requests

object ShopFlink {

  def main(args: Array[String]): Unit = {

    //获取flink执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //kafka属性
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ELK01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //flink 一般由三部分组成 1.source 2.算子 3.sink

    //1.source输入---kafka作为source
    //入参 topic SimpleStringSchema--读取kafka消息是string格式 properties kafka的配置
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("shop1", new SimpleStringSchema(), properties))

    //2.1算子--处理数据
    val stream = inputStream.map(new MapFunction[String,JSONObject] {
      override def map(value: String): JSONObject = {
        val jsonObject: JSONObject = JSON.parseObject(value)
        jsonObject
      }
    }).filter(new FilterFunction[JSONObject]() {
      @throws[Exception]
      override def filter(value: JSONObject): Boolean = {
        value.containsKey("mtWmPoiId")
      }
    }).map(new MapFunction[JSONObject,Shop] {
      override def map(value: JSONObject): Shop = {
        var shopBean: Shop = null
        try {
          shopBean = dealShop(value)
        } catch {
          case e: JSONException => {
          }
        }
        shopBean
      }
    }).filter(new FilterFunction[Shop] {
      override def filter(t: Shop): Boolean = {
        null!=t
      }
    }).assignTimestampsAndWatermarks(new MyCustomerAssigner())

    // hdfs 计算 --处理成能保存hdfs的数据格式流
    val hdfsStream = stream.map(new MapFunction[Shop,String] {
        override def map(shopBean:Shop): String = {
          val shopLine:String = dealShopToLineString(shopBean)
          shopLine
        }
      })

    // es 计算 处理成能保存进es的数据格式流
    val esStream = stream.map(new MapFunction[Shop,String] {
      override def map(shop: Shop): String = {
        val conf = new SerializeConfig(true)
        val shopJson = JSON.toJSONString(shop, conf)
        shopJson
      }
    })

    //3.sink输出---hdfs作为sink
    val outputPath ="hdfs://ELK01:9000/user/hive/warehouse/wm.db/ods_shop_flink";

    // BucketAssigner --分桶策略 默认每小时生成一个文件夹
    // RollingPolicy -- 分件滚动策略
    // --withInactivityInterval --最近30分钟没有收到新的记录  withRolloverInterval --它至少包含 60 分钟的数据
    // --withMaxPartSize 文件大小达到多少
    val sink = StreamingFileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      // 采用的是自定义的分桶类，把文件保存到某个表的目录下，按照hive分区目录命名方式生成文件名
      .withBucketAssigner(new BatchDateBucketAssigner[String])
      .withRollingPolicy(DefaultRollingPolicy.create()
      .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
      .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
      .withMaxPartSize(1024 * 1024 )
      .build())
      .build()
    hdfsStream.addSink(sink)

    //3.sink输出---es作为sink
    //es配置属性
    val config = new java.util.HashMap[String,String]()
    //集群名称
    config.put("cluster.name", "elk")
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")
    //地址
    val transportAddresses = new java.util.ArrayList[InetSocketAddress]()
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))
    val currentDate = getNowDate("yyyy-MM-dd")
    esStream.addSink(new ElasticsearchSink[String](config,transportAddresses,new ElasticsearchSinkFunction[String] {
      override def process(shopJson: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val request = Requests.indexRequest().index("shop").`type`(currentDate).source(shopJson)
        requestIndexer.add(request)
      }
    }))

    env.execute()
  }


  /**
    * 处理店铺信息
    * @param shop
    * @return
    */
  def dealShopToLineString(shop:Shop):String={

    val builder = new StringBuilder()
    val shopLine = builder.append(shop.dp_shop_id).append("\t").append(shop.wm_poi_id).append("\t")
      .append(shop.shop_name).append("\t").append(shop.shop_status).append("\t")
      .append(shop.shop_pic).append("\t").append(shop.delivery_fee).append("\t")
      .append(shop.delivery_time).append("\t").append(shop.delivery_type).append("\t")
      .append(shop.min_fee).append("\t").append(shop.online_pay).append("\t")
      .append(shop.shipping_time).append("\t").append(shop.bulletin).append("\t")
      .append(shop.create_time)
    shopLine.toString()
  }
  /**
    * 处理店铺信息
    * @param jsonObject
    * @return
    */
  def dealShop(jsonObject: JSONObject):Shop={
    val mtWmPoiId = jsonObject.getString("mtWmPoiId")
    val dpShopId = jsonObject.getIntValue("dpShopId")
    val shopStatus = jsonObject.getIntValue("shopStatus")
    val shopName = jsonObject.getString("shopName")
    val shopPic = jsonObject.getString("shopPic")
    val deliveryFee = jsonObject.getString("deliveryFee")
    val deliveryType = jsonObject.getString("deliveryType")
    val deliveryTime = jsonObject.getString("deliveryTime")
    val minFee = jsonObject.getDouble("minFee")
    val onlinePay = jsonObject.getInteger("onlinePay")
    val bulletin = jsonObject.getString("bulletin")
    val shipping_time = jsonObject.getString("shipping_time")
    val create_time = getNowDate("yyyy-MM-dd HH:mm:ss")
    Shop(mtWmPoiId,dpShopId,shopName,shopStatus,shopPic,deliveryFee,deliveryType,
      deliveryTime,minFee,onlinePay,bulletin,shipping_time,create_time)
  }

  def getNowDate(format: String): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    var time = dateFormat.format(now)
    time
  }

  case class Shop(wm_poi_id: String, dp_shop_id: Int, shop_name: String, shop_status: Int, shop_pic: String
                  , delivery_fee: String, delivery_type: String, delivery_time: String, min_fee: Double,
                  online_pay: Int, bulletin: String, shipping_time: String, create_time: String)


  //自定义eventTime
  class MyCustomerAssigner() extends AssignerWithPeriodicWatermarks[Shop]{

    val maxOutOfOrderness = 3500L // 3.5 seconds

    var currentMaxTimestamp: Long = _

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    override def getCurrentWatermark: Watermark = {
      // return the watermark as current highest timestamp minus the out-of-orderness bound
      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }

    def max(timestamp: Long, currentMaxTimestamp: Long): Long = {
      math.max(timestamp,currentMaxTimestamp)
    }

    override def extractTimestamp(shop: Shop, previousElementTimestamp: Long): Long = {
      var create_time = shop.create_time
      if(null==create_time){
        create_time=getNowDate("yyyy-MM-dd HH:mm:ss")
      }
      val timestamp = dateFormat.parse(create_time).getTime
      currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
      timestamp
    }
  }

}





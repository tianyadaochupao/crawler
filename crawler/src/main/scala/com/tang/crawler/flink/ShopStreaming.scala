package com.tang.crawler.flink

import java.text.SimpleDateFormat
import java.time.{Duration, ZoneId}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.api.scala._

object ShopStreaming {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    //设置tableConfig 用户可以通过 table.local-time-zone 自行设置
    val tableConfig = tableEnv.getConfig
    tableConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)

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
    import org.apache.flink.streaming.api.scala._
    val  kafkaSource: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("shop1", new SimpleStringSchema(), properties)
    val inputStream = env.addSource(kafkaSource)

    val stream: DataStream[Shop] = inputStream.map(new MapFunction[String,Shop] {
      override def map(value: String): Shop = {
        val jsonObject: JSONObject = JSON.parseObject(value)
        val shop: Shop = dealShop(jsonObject)
        shop
      }
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Shop](Duration.ofSeconds(20))
      .withTimestampAssigner(new SerializableTimestampAssigner[Shop] {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        override def extractTimestamp(element: Shop, recordTimestamp: Long): Long = {
          val create_time = element.create_time
          val date = dateFormat.parse(create_time)
          date.getTime
        }
      }))
    val name            = "myHive"
    val defaultDatabase = "wm"
    val hiveConfDir     = "D:\\tang-spark2\\crawler\\src\\main\\resources"
    val version         = "2.3.4"
    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myHive", hive)
    tableEnv.useCatalog("myHive")
    tableEnv.useDatabase("wm")
    //流转为表
    tableEnv.createTemporaryView("shop",stream)

    tableEnv.executeSql("""
                          |INSERT INTO ods_shop
                          |SELECT wm_poi_id,dp_shop_id,shop_name,shop_status,shop_pic,delivery_fee,delivery_type,
                          |delivery_time,min_fee,online_pay,bulletin,shipping_time,create_time,dt,hr,mm
                          |FROM shop
                        """.stripMargin)
    print("运行成功")
    env.execute()
  }

  def formatDateByFormat (dateString:String,format: String ): String = {

    val sourceDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val date = sourceDateFormat.parse(dateString)
    val targetDateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val time = targetDateFormat.format(date)
    time
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
    val create_time = jsonObject.getString("createTime")
    val dt = formatDateByFormat(create_time,"yyyy-MM-dd")
    val hr = formatDateByFormat(create_time,"HH")
    val mm = formatDateByFormat(create_time,"mm")
    Shop(mtWmPoiId,dpShopId,shopName,shopStatus,shopPic,deliveryFee,deliveryType,
      deliveryTime,minFee,onlinePay,bulletin,shipping_time,create_time,dt,hr,mm)
  }

  case class Item(wm_poi_id:String, dp_shop_id:Int,shop_name:String,spu_id:Long, spu_name:String, unit:String,tag:String,activity_tag:String
                  ,little_image:String ,big_image:String ,origin_price:Double ,current_price:Double,
                  spu_desc:String,praise_num:Long,activity_type:Long,spu_promotion_info:String,status_desc:String,
                  category_name:String,category_type:String,create_time:String)

  case class Coupon(wm_poi_id:String, dp_shop_id:Int,shop_name:String,coupon_id:Long, coupon_pool_id:Long, activity_id:Long,coupon_value:Double,coupon_condition_text:String
                    ,coupon_valid_time_text:String ,coupon_status:String ,limit_new_user:String,create_time:String )


  case class Shop(wm_poi_id:String, dp_shop_id:Int,shop_name:String,shop_status:Int,shop_pic:String
                  ,delivery_fee:String ,delivery_type:String ,delivery_time:String ,min_fee:Double,
                  online_pay:Int,bulletin:String,shipping_time:String,create_time:String,dt:String,hr:String,mm:String)

}

package com.tang.crawler.wm

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.rdd.EsSpark

object ShopListStreaming {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("kafka_Direct_streaming").setMaster("local[4]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val session = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()

    ssc.checkpoint("F:\\checkpoint\\kafka-direct")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ELK01:9092,ELK02:9092,ELK03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "shopGroup",
      "auto.offset.reset" -> "latest",
      "max.poll.records" -> 10.toString,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("shop_list1")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val shopBasicDS = stream.map(_.value())

    shopBasicDS.foreachRDD(rdd=>{
      //保存到hive中
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      saveToHive(rdd,sparkSession)
      //保存到es中
      saveToEs(rdd)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def saveToHive(rdd:RDD[String],sparkSession: SparkSession)={
    if(!rdd.isEmpty()){
      import sparkSession.implicits._
      val curentDate = getNowDate()
      //商品信息
      val shopBasicDataFrame = rdd.map(JSON.parseArray).flatMap(_.toArray()).map(x=>{
        val jsonObject = JSON.parseObject(x.toString)
        jsonObject
      }).map(x=>{
        var shopBasicBean: ShopBasic =null
        try {
          shopBasicBean =dealShopBasic(x)
        } catch {
          case e: JSONException => {
          }
        }
        shopBasicBean
      }).filter(null!=_).toDF()
      val tableName = "shop_basic"
      shopBasicDataFrame.createOrReplaceTempView(tableName)
      //存入表中
      sparkSession.sql("use wm")
      sparkSession.sql("set hive.exec.dynamici.partition=true")
      sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sparkSession.sql("insert into table ods_shop_basic partition(batch_date) " +
        s"select wm_poi_id,shop_name,status_desc,address,pic_url,wm_poi_score,shipping_time,status,create_time,$curentDate from shop_basic")
    }else{
      println("save to hive rdd is empty")
    }
  }

  /**
    * 保存到elasticsearch
    * @param rdd
    */
  def saveToEs(rdd:RDD[String])={

    if(!rdd.isEmpty()){
      val curentDate = getNowDate()
      val shopRdd = rdd.map(JSON.parseArray).flatMap(_.toArray()).map(x=>{
        JSON.parseObject(x.toString)
      }).map(x=>{
        val jsonObject = new JSONObject()
        jsonObject.put("wm_poi_id",x.getString("mtWmPoiId"))
        jsonObject.put("shop_name",x.getString("shopName"))
        jsonObject.put("status_desc",x.getString("statusDesc"))
        jsonObject.put("address",x.getString("address"))
        jsonObject.put("pic_url",x.getString("picUrl"))
        jsonObject.put("wm_poi_score",x.getIntValue("wmPoiScore"))
        jsonObject.put("shipping_time",x.getString("shipping_time"))
        jsonObject.put("status",x.getIntValue("status"))
        jsonObject.put("create_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
        jsonObject.put("update_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
        jsonObject
      })
      EsSpark.saveToEs(shopRdd,s"shop_basic/$curentDate")
    }else{
      println("save to es rdd is empty")
    }
  }


  /**
    * 处理店铺基本信息
    * @param jsonObject
    * @return
    */
  def dealShopBasic(jsonObject: JSONObject):ShopBasic={

    val mtWmPoiId = jsonObject.getString("mtWmPoiId")
    val shopName = jsonObject.getString("shopName")
    val statusDesc = jsonObject.getString("statusDesc")
    val address = jsonObject.getString("address")
    val picUrl = jsonObject.getString("picUrl")
    val wmPoiScore = jsonObject.getIntValue("wmPoiScore")
    val shipping_time = jsonObject.getString("shipping_time")
    val status = jsonObject.getIntValue("status")
    val create_time = getNowDate("yyyy-MM-dd HH:mm:ss")
    ShopBasic(mtWmPoiId,shopName,statusDesc,address,picUrl,wmPoiScore,shipping_time,status,create_time)
  }

  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var time = dateFormat.format( now )
    time
  }
  def getNowDate(format:String):String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat(format)
    var time = dateFormat.format( now )
    time
  }

  case class ShopBasic(wm_poi_id:String, shop_name:String,status_desc:String, address:String
                       ,pic_url:String ,wm_poi_score:Int ,shipping_time:String,status:Int,create_time:String)

}

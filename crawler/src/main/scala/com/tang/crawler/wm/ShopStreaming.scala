package com.tang.crawler.wm

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.tang.crawler.utils.SparkSessionSingleton
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object ShopStreaming {

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
    val topics = Array("shop1")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val shopDS = stream.map(_.value())

    shopDS.foreachRDD(rdd=>{
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
      //保存店铺信息到hive
      try{
        saveShopToHive(rdd,sparkSession)
        //保存商品信息到hive
        saveItemToHive(rdd,sparkSession)
        saveCouponToHive(rdd,sparkSession)
      }catch {
        case e:Exception=>{}

      }
    }
  }


  /**
    * 保存店铺信息
    * @param rdd
    * @param sparkSession
    */
  def saveShopToHive(rdd:RDD[String],sparkSession: SparkSession):Unit={
    import sparkSession.implicits._
    val curentDate = getNowDate()
    //店铺信息
    val shopDataFrame = rdd.map(x=>{
      val jsonObject = JSON.parseObject(x)
      jsonObject
    }).filter(x=>{
      x.containsKey("mtWmPoiId")
    }).map(x=>{
      var shopBean: Shop =null
      try {
        shopBean =dealShop(x)
      } catch {
        case e: JSONException => {
        }
      }
      shopBean
    }).toDF()
    val tableName = "shop"
    shopDataFrame.createOrReplaceTempView(tableName)
    //存入表中
    sparkSession.sql("use wm")
    sparkSession.sql("set hive.exec.dynamici.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql("insert into table ods_shop partition(batch_date) " +
      "select wm_poi_id,dp_shop_id,shop_name,shop_status,shop_pic,delivery_fee," +
      s"delivery_type,delivery_time,min_fee,online_pay,bulletin,shipping_time,create_time,$curentDate from shop")
  }


  /**
    * 保存店铺信息
    * @param rdd
    */
  def saveShopToEs(rdd:RDD[String]):Unit={
    val curentDate = getNowDate()
    if(!rdd.isEmpty()){
       val shopRdd = rdd.map(x=>{
         val jsonObject = JSON.parseObject(x)
         jsonObject
       }).filter(x=>{
         x.containsKey("mtWmPoiId")
       }).map(x=>{
         val jsonObject = new JSONObject()
         jsonObject.put("wm_poi_id",x.getString("mtWmPoiId"))
         jsonObject.put("dp_shop_id",x.getIntValue("dpShopId"))
         jsonObject.put("shop_name",x.getString("shopName"))
         jsonObject.put("shop_status",x.getIntValue("shopStatus"))
         jsonObject.put("shop_pic",x.getString("shopPic"))
         jsonObject.put("delivery_fee",x.getIntValue("deliveryFee"))
         jsonObject.put("delivery_type",x.getIntValue("deliveryType"))
         jsonObject.put("delivery_time",x.getString("deliveryTime"))
         jsonObject.put("delivery_msg",x.getString("deliveryMsg"))
         jsonObject.put("min_fee",x.getDoubleValue("minFee"))
         jsonObject.put("online_pay",x.getIntValue("onlinePay"))
         jsonObject.put("bulletin",x.getString("bulletin"))
         jsonObject.put("shipping_time",x.getString("shipping_time"))
         jsonObject.put("create_time",x.getString("createTime"))
         jsonObject.put("update_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
         jsonObject
       })
      EsSpark.saveToEs(shopRdd,s"shop/$curentDate")
    }
  }
  /**
    * 保存商品信息
    * @param rdd
    */
  def saveItemToEs(rdd:RDD[String]):Unit={
    val curentDate = getNowDate()
    val itemRdd = rdd.map(x=>{
       val jsonObject = JSON.parseObject(x)
       jsonObject
     }).filter(x=>{
       null!=x.get("itemList")
     }).map( x=>{
      val items = x.getString("itemList")
      items
    }).map(JSON.parseArray).flatMap(_.toArray).map(x=>{
      val jsonObject = JSON.parseObject(x.toString)
      val item = new JSONObject()
      item.put("wm_poi_id",jsonObject.getString("mtWmPoiId"))
      item.put("dp_shop_id",jsonObject.getIntValue("dpShopId"))
      item.put("shop_name",jsonObject.getString("shopName"))
      item.put("spu_id",jsonObject.getLongValue("spuId"))
      item.put("spu_name",jsonObject.getString("spuName"))
      item.put("unit",jsonObject.getString("unit"))
      item.put("tag",jsonObject.getString("tag"))
      item.put("activity_tag",jsonObject.getString("activityTag"))
      item.put("little_image",jsonObject.getString("littleImageUrl"))
      item.put("big_image",jsonObject.getString("bigImageUrl"))
      item.put("origin_price",jsonObject.getDoubleValue("originPrice"))
      item.put("current_price",jsonObject.getDoubleValue("currentPrice"))
      item.put("spu_desc",jsonObject.getString("spuDesc"))
      item.put("praise_num",jsonObject.getLongValue("praiseNum"))
      item.put("activity_type",jsonObject.getLongValue("activityType"))
      item.put("spu_promotion_info",jsonObject.getString("spuPromotionInfo"))
      item.put("status_desc",jsonObject.getString("statusDesc"))
      item.put("category_name",jsonObject.getString("categoryName"))
      item.put("category_type",jsonObject.getString("categoryType"))
      item.put("create_time",jsonObject.getString("createTime"))
      item.put("update_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
      item
    })
    EsSpark.saveToEs(itemRdd,s"item/$curentDate")
  }

  /**
    * 保存店铺优惠信息到es
    * @param rdd
    */
  def saveCouponToEs(rdd:RDD[String]):Unit={
    val curentDate = getNowDate()
    val couponRdd = rdd.map(x=>{
       val jsonObject = JSON.parseObject(x)
       jsonObject
     }).filter(x=>{
       null!=x.get("couponList")
     }).map( x=>{
      val coupons = x.getString("couponList")
      coupons
    }).map(JSON.parseArray).flatMap(_.toArray).map(x=>{
      val jsonObject = JSON.parseObject(x.toString)
      val coupon = new JSONObject()
      coupon.put("wm_poi_id",jsonObject.getString("mtWmPoiId"))
      coupon.put("dp_shop_id",jsonObject.getIntValue("dpShopId"))
      coupon.put("shop_name",jsonObject.getString("shopName"))
      coupon.put("coupon_id",jsonObject.getLongValue("couponId"))
      coupon.put("coupon_pool_id",jsonObject.getLongValue("couponPoolId"))
      coupon.put("activity_id",jsonObject.getLongValue("activityId"))
      coupon.put("coupon_value",jsonObject.getDoubleValue("couponValue"))
      coupon.put("coupon_condition_text",jsonObject.getString("couponConditionText"))
      coupon.put("coupon_valid_time_text",jsonObject.getString("couponValidTimeText"))
      coupon.put("coupon_status",jsonObject.getString("couponStatus"))
      coupon.put("limit_new_user",jsonObject.getString("limitNewUser"))
      coupon.put("create_time",jsonObject.getString("createTime"))
      coupon.put("update_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
      coupon
    })
    EsSpark.saveToEs(couponRdd,s"coupon/$curentDate")

  }

  /**
    * 保存商品信息
    * @param rdd
    * @param sparkSession
    */
  def saveItemToHive(rdd:RDD[String],sparkSession: SparkSession):Unit={

    import sparkSession.implicits._
    val curentDate = getNowDate()
    //商品信息
    val itemDataFrame = rdd.map(x=>{
      val jsonObject = JSON.parseObject(x)
      jsonObject
    }).filter(x=>{
      null!=x.get("itemList")
    }).map(x=>{
      val items = x.getString("itemList")
      items
    }).map(JSON.parseArray).flatMap(_.toArray).map(x=>{
      var jsonObject: JSONObject =null
      var itemBean: Item =null
      try {
        val jsonObject = JSON.parseObject(x.toString)
        itemBean=dealItem(jsonObject)
      } catch {
        case e: JSONException => {
        }
      }
      itemBean
    }).toDF()
    val tableName = "item"
    itemDataFrame.createOrReplaceTempView(tableName)
    //存入表中
    sparkSession.sql("use wm")
    sparkSession.sql("set hive.exec.dynamici.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql("insert into table ods_item partition(batch_date) " +
      "select wm_poi_id,dp_shop_id,shop_name,spu_id,spu_name," +
      "unit,tag,activity_tag,little_image,big_image,origin_price," +
      "current_price,spu_desc,praise_num,activity_type,spu_promotion_info,status_desc," +
      s"category_name,category_type,create_time,$curentDate from item")

  }


  /**
    * 保存店铺优惠劵信息
    * @param rdd
    * @param sparkSession
    */
  def saveCouponToHive(rdd:RDD[String],sparkSession: SparkSession):Unit={

    import sparkSession.implicits._
    val curentDate = getNowDate()
    //商品信息
    val itemDataFrame = rdd.map(x=>{
      val jsonObject = JSON.parseObject(x)
      jsonObject
    }).filter(x=>{
      null!=x.get("couponList")
    }).map(x=>{
      val items = x.getString("couponList")
      items
    }).map(JSON.parseArray).flatMap(_.toArray).map(x=>{
      var jsonObject: JSONObject =null
      var couponBean: Coupon =null
      try {
        val jsonObject = JSON.parseObject(x.toString)
        couponBean=dealCoupon(jsonObject)
      } catch {
        case e: JSONException => {
        }
      }
      couponBean
    }).toDF()
    val tableName = "coupon"
    itemDataFrame.createOrReplaceTempView(tableName)
    //存入表中
    sparkSession.sql("use wm")
    sparkSession.sql("set hive.exec.dynamici.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql("insert into table ods_coupon partition(batch_date) " +
      "select wm_poi_id,dp_shop_id,shop_name,coupon_id,coupon_pool_id,activity_id," +
      s"coupon_value,coupon_condition_text,coupon_valid_time_text,coupon_status,limit_new_user,create_time,$curentDate from coupon")
  }


  /**
    * 保存到elasticsearch
    * @param rdd
    */
  def saveToEs(rdd:RDD[String])={

    if(!rdd.isEmpty()){
      try{
        saveShopToEs(rdd)
        saveItemToEs(rdd)
        saveCouponToEs(rdd)
      }catch {
        case e:Exception=>{}
      }
    }else{
      println("save to es rdd is empty")
    }
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
    Shop(mtWmPoiId,dpShopId,shopName,shopStatus,shopPic,deliveryFee,deliveryType,
      deliveryTime,minFee,onlinePay,bulletin,shipping_time,create_time)
  }
  /**
    * 处理店铺信息
    * @param jsonObject
    * @return
    */
  def dealItem(jsonObject: JSONObject):Item={
    val mtWmPoiId = jsonObject.getString("mtWmPoiId")
    val dpShopId = jsonObject.getIntValue("dpShopId")
    val shopName = jsonObject.getString("shopName")
    val spuId = jsonObject.getLongValue("spuId")
    val spuName = jsonObject.getString("spuName")
    val unit = jsonObject.getString("unit")
    val tag = jsonObject.getString("tag")
    val activityTag = jsonObject.getString("activityTag")
    val littleImageUrl = jsonObject.getString("littleImageUrl")
    val bigImageUrl = jsonObject.getString("bigImageUrl")
    val originPrice = jsonObject.getDoubleValue("originPrice")
    val currentPrice = jsonObject.getDoubleValue("currentPrice")
    val spuDesc = jsonObject.getString("spuDesc")
    val praiseNum = jsonObject.getLongValue("praiseNum")
    val activityType = jsonObject.getLongValue("activityType")
    val spuPromotionInfo = jsonObject.getString("spuPromotionInfo")
    val statusDesc = jsonObject.getString("statusDesc")
    val categoryName = jsonObject.getString("categoryName")
    val categoryType = jsonObject.getString("categoryType")
    val create_time = jsonObject.getString("createTime")
    Item(mtWmPoiId,dpShopId,shopName,spuId,spuName,unit,tag,activityTag,littleImageUrl,bigImageUrl,originPrice,
      currentPrice,spuDesc,praiseNum,activityType,spuPromotionInfo,statusDesc,categoryName,categoryType,create_time)
  }

  /**
    * 处理店铺优惠劵
    * @param jsonObject
    * @return
    */
  def dealCoupon(jsonObject: JSONObject):Coupon={
    val mtWmPoiId = jsonObject.getString("mtWmPoiId")
    val dpShopId = jsonObject.getIntValue("dpShopId")
    val shopName = jsonObject.getString("shopName")
    val couponId = jsonObject.getLongValue("couponId")
    val couponPoolId = jsonObject.getLongValue("couponPoolId")
    val activityId = jsonObject.getLongValue("activityId")
    val couponValue = jsonObject.getDoubleValue("couponValue")
    val couponConditionText = jsonObject.getString("couponConditionText")
    val couponValidTimeText = jsonObject.getString("couponValidTimeText")
    val couponStatus = jsonObject.getString("couponStatus")
    val limitNewUser = jsonObject.getString("limitNewUser")
    val create_time = jsonObject.getString("createTime")
    Coupon(mtWmPoiId,dpShopId,shopName,couponId,couponPoolId,activityId,couponValue,couponConditionText,couponValidTimeText,couponStatus,limitNewUser,create_time)
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


  case class Item(wm_poi_id:String, dp_shop_id:Int,shop_name:String,spu_id:Long, spu_name:String, unit:String,tag:String,activity_tag:String
                  ,little_image:String ,big_image:String ,origin_price:Double ,current_price:Double,
                  spu_desc:String,praise_num:Long,activity_type:Long,spu_promotion_info:String,status_desc:String,
                  category_name:String,category_type:String,create_time:String)

  case class Coupon(wm_poi_id:String, dp_shop_id:Int,shop_name:String,coupon_id:Long, coupon_pool_id:Long, activity_id:Long,coupon_value:Double,coupon_condition_text:String
                  ,coupon_valid_time_text:String ,coupon_status:String ,limit_new_user:String,create_time:String )


  case class Shop(wm_poi_id:String, dp_shop_id:Int,shop_name:String,shop_status:Int,shop_pic:String
                  ,delivery_fee:String ,delivery_type:String ,delivery_time:String ,min_fee:Double,
                  online_pay:Int,bulletin:String,shipping_time:String,create_time:String)

}

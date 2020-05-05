package com.tang.crawler.wm

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.tang.crawler.utils.SparkSessionSingleton
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.rdd.EsSpark

object ShopCommentStreaming {


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
    val topics = Array("comment1")

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
      try {
        //保存店铺信息到hive
        saveShopCommentToHive(rdd,sparkSession)
        //保存商品信息到hive
        saveUserCommentToHive(rdd,sparkSession)
      }catch {
        case e:Exception=>{

        }
      }
    }
  }


  /**
    * 保存店铺信息
    * @param rdd
    * @param sparkSession
    */
  def saveShopCommentToHive(rdd:RDD[String],sparkSession: SparkSession):Unit={
    import sparkSession.implicits._
    val curentDate = getNowDate()
    //店铺信息
    val shopCommentDataFrame = rdd.map(x=>{
      val jsonObject = JSON.parseObject(x)
      jsonObject
    }).filter(x=>{
      x.containsKey("mtWmPoiId")
    }).map(x=>{
      var shopCommentBean: ShopComment =null
      try {
        shopCommentBean =dealShopComment(x)
      } catch {
        case e: JSONException => {
        }
      }
      shopCommentBean
    }).filter(null!=_).toDF()
    val tableName = "shop_comment"
    shopCommentDataFrame.createOrReplaceTempView(tableName)
    //存入表中
    sparkSession.sql("use wm")
    sparkSession.sql("set hive.exec.dynamici.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql("insert into table ods_shop_comment partition(batch_date) " +
      "select shop_id,wm_poi_id,record_count,dp_comment_num,praise_ratio,quality_score,pack_score," +
      s"integrated_score,shop_score,delivery_score,comment_labels,create_time,$curentDate from shop_comment")
  }


  /**
    * 保存店铺评价到es
    * @param rdd
    */
  def saveShopCommentToEs(rdd:RDD[String]):Unit={
    val curentDate = getNowDate()
    if(!rdd.isEmpty()){
      val shopCommentRdd = rdd.map(x=>{
        val jsonObject = JSON.parseObject(x)
        jsonObject
      }).filter(x=>{
        x.containsKey("mtWmPoiId")
      }).map(x=>{
        val jsonObject = dealShopCommentToJson(x)
        jsonObject
      })
      EsSpark.saveToEs(shopCommentRdd,s"shop_comment/$curentDate")
    }
  }
  /**
    * 保存商品信息
    * @param rdd
    */
  def saveUserCommentToEs(rdd:RDD[String]):Unit={
    val curentDate = getNowDate()
    val userCommentRdd = rdd.map(x=>{
      val jsonObject = JSON.parseObject(x)
      jsonObject
    }).filter(x=>{
      null!=x.get("list")
    }).map( x=>{
      val items = x.getString("list")
      items
    }).map(JSON.parseArray).flatMap(_.toArray).map(x=>{
      val jsonObject = JSON.parseObject(x.toString)
      val userComment = dealUserCommentToJson(jsonObject)
      userComment
    })
    EsSpark.saveToEs(userCommentRdd,s"user_comment/$curentDate")

  }


  /**
    * 用户评论信息
    * @param rdd
    * @param sparkSession
    */
  def saveUserCommentToHive(rdd:RDD[String],sparkSession: SparkSession):Unit={

    import sparkSession.implicits._
    val curentDate = getNowDate()
    //商品信息
    val commentDataFrame = rdd.map(x=>{
      val jsonObject = JSON.parseObject(x)
      jsonObject
    }).filter(x=>{
      null!=x.get("list")
    }).map( x=>{
      val comments = x.getString("list")
      comments
    }).map(JSON.parseArray).flatMap(_.toArray).
      map(x=>{
        var userCommentBean: UserComment =null
        try {
          val jsonObject = JSON.parseObject(x.toString)
          userCommentBean=dealUserComment(jsonObject)
        } catch {
          case e: JSONException => {
          }
        }
        userCommentBean
      }).toDF()
    val tableName = "user_comment"
    commentDataFrame.createOrReplaceTempView(tableName)
    //存入表中
    sparkSession.sql("use wm")
    sparkSession.sql("set hive.exec.dynamici.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql("insert into table ods_user_comment partition(batch_date) " +
      "select shop_id,wm_poi_id,user_id,user_name,user_pic_url,comment_time,score," +
      s"delivery_time,content,shop_reply,is_anonymous,pictures,create_time,$curentDate from user_comment")
  }

  /**
    * 保存到elasticsearch
    * @param rdd
    */
  def saveToEs(rdd:RDD[String])={

    if(!rdd.isEmpty()){
      try{
        saveShopCommentToEs(rdd)
        saveUserCommentToEs(rdd)
      }catch {
        case e:Exception=>{
        }
      }
    }else{
      println("save to es rdd is empty")
    }
  }


  /**
    * 处理店铺评论信息
    * @param jsonObject
    * @return
    */
  def dealShopComment(jsonObject: JSONObject):ShopComment={

    val shopId = jsonObject.getLong("shopId")
    val mtWmPoiId = jsonObject.getString("mtWmPoiId")
    val recordCount = jsonObject.getLongValue("recordCount")
    val dpCommentNum = jsonObject.getLongValue("dpCommentNum")
    val praiseRatio = jsonObject.getString("PraiseRatio")
    val qualityScore = jsonObject.getDoubleValue("qualityScore")
    val packScore = jsonObject.getDoubleValue("packScore")
    val integratedScore = jsonObject.getDoubleValue("integratedScore")
    val shopScore = jsonObject.getDoubleValue("shopScore")
    val deliveryScore = jsonObject.getDoubleValue("deliveryScore")
    val commentLabels = jsonObject.getString("commentLabels")
    val create_time = getNowDate("yyyy-MM-dd HH:mm:ss")
    ShopComment(shopId,mtWmPoiId,recordCount,dpCommentNum,praiseRatio,qualityScore,packScore,integratedScore,shopScore,deliveryScore,commentLabels,create_time)
  }

  /**
    * 处理店铺评论信息成jsonObject
    * @param jsonObject
    * @return
    */
  def dealShopCommentToJson(jsonObject: JSONObject):JSONObject={

    val shopComment = new JSONObject()
    shopComment.put("shop_id",jsonObject.getLong("shopId"))
    shopComment.put("wm_poi_id",jsonObject.getString("mtWmPoiId"))
    shopComment.put("record_count",jsonObject.getLongValue("recordCount"))
    shopComment.put("dp_comment_num",jsonObject.getLongValue("dpCommentNum"))
    shopComment.put("praise_ratio",jsonObject.getString("PraiseRatio"))
    shopComment.put("quality_score",jsonObject.getDoubleValue("qualityScore"))
    shopComment.put("pack_score",jsonObject.getDoubleValue("packScore"))
    shopComment.put("integrated_score",jsonObject.getDoubleValue("integratedScore"))
    shopComment.put("shop_score",jsonObject.getDoubleValue("shopScore"))
    shopComment.put("delivery_score",jsonObject.getDoubleValue("deliveryScore"))
    shopComment.put("comment_labels",jsonObject.getString("commentLabels"))
    shopComment.put("create_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
    shopComment.put("update_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
    shopComment
  }

  /**
    * 处理用户评论
    * @param jsonObject
    * @return
    */
  def dealUserComment(jsonObject: JSONObject):UserComment={

    val shopId = jsonObject.getLongValue("shopId")
    val mtWmPoiId = jsonObject.getString("mtWmPoiId")
    val userID = jsonObject.getLongValue("userID")
    val userName = jsonObject.getString("userName")
    val userPicUrl = jsonObject.getString("userPicUrl")
    val commentTime = jsonObject.getString("commentTime")
    val score = jsonObject.getLongValue("score")
    val deliveryTime = jsonObject.getString("deliveryTime")
    val content = jsonObject.getString("content")
    val shopReply = jsonObject.getString("shopReply")
    val isAnonymous = jsonObject.getLongValue("isAnonymous")
    //这里取的是用户评论的原图
    val pictures = jsonObject.getString("originalPicUrls")
    val create_time =getNowDate("yyyy-MM-dd HH:mm:ss")
    UserComment(shopId,mtWmPoiId,userID,userName,userPicUrl,commentTime,score,deliveryTime,content,shopReply,isAnonymous,pictures,create_time)
  }

  /**
    * 处理用户评论
    * @param jsonObject
    * @return
    */
  def dealUserCommentToJson(jsonObject: JSONObject):JSONObject={

    val userComment = new JSONObject()
    userComment.put("shop_id",jsonObject.getLongValue("shopId"))
    userComment.put("wm_poi_id",jsonObject.getString("mtWmPoiId"))
    userComment.put("user_id",jsonObject.getLongValue("userID"))
    userComment.put("user_name",jsonObject.getString("userName"))
    userComment.put("user_pic_url",jsonObject.getString("userPicUrl"))
    userComment.put("comment_time",jsonObject.getString("commentTime"))
    userComment.put("score",jsonObject.getLongValue("score"))
    userComment.put("delivery_time",jsonObject.getString("deliveryTime"))
    userComment.put("content",jsonObject.getString("content"))
    userComment.put("shop_reply",jsonObject.getString("shopReply"))
    userComment.put("is_anonymous",jsonObject.getLongValue("isAnonymous"))
    //这里取的是用户评论的原图
    userComment.put("pictures",jsonObject.getString("originalPicUrls"))
    userComment.put("create_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
    userComment.put("update_time",getNowDate("yyyy-MM-dd HH:mm:ss"))
    userComment
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

  case class ShopComment(shop_id:Long, wm_poi_id:String,record_count:Long, dp_comment_num:Long
                         ,praise_ratio:String ,quality_score:Double ,pack_score:Double,integrated_score:Double,
                         shop_score:Double,delivery_score:Double,comment_labels:String,create_time:String)

  case class UserComment(shop_id:Long, wm_poi_id:String,user_id:Long, user_name:String
                         ,user_pic_url:String ,comment_time:String ,score:Double,delivery_time:String,
                         content:String,shop_reply:String,is_anonymous:Long,pictures:String,create_time:String)


}

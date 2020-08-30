package com.tang.crawler.spark

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object SparkSqlShopItemToCsv {

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("shop")
      .master("local[4]")
      .getOrCreate()
    import session.implicits._
    val recordRDD:DataFrame = session.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost/wm", "driver" -> "com.mysql.jdbc.Driver"
      , "dbtable" -> "record", "user" -> "root", "password" -> "123456")).load()
    recordRDD.createOrReplaceTempView("record")
    //读取record表中的数据
    val recordDf = session.sql("select a.response,a.request,a.create_time from record a where a.create_time >'2020-06-13 22:00:30'")

    val currentDate = getNowDate("yyyy-MM-dd HH:mm:ss")
    import scala.collection.JavaConversions._
    //对record 中的数据进行处理
    val ds = recordDf.map(one => {
      //获取response字段的值
      val response = one.getAs[String]("response")
      val recordJsonObject = JSON.parseObject(response)
      val mtWmPoiId = recordJsonObject.getString("mtWmPoiId")
      val shopName = recordJsonObject.getString("shopName")
      //店铺
      val shop = new Shop(mtWmPoiId, shopName, currentDate)
      val itemArray: JSONArray =recordJsonObject.getJSONArray("itemList")
      val itemObjectList: List[AnyRef] = itemArray.iterator().toList

      val itemListBuffer = ListBuffer[Item]()
      val relationListBuffer = ListBuffer[Relation]()
      for(x <-itemObjectList){
        val itemJsonObject = JSON.parseObject(x.toString)
        val spuId = itemJsonObject.getLong("spuId")
        val spuName = itemJsonObject.getString("spuName")
        //商品
        val item = new Item(spuId, spuName, currentDate)
        itemListBuffer.append(item)
        //店铺与商品关系
        val relation = new Relation(mtWmPoiId, shopName, spuId, spuName, currentDate)
        relationListBuffer.append(relation)
      }
      val itemList = itemListBuffer.toList
      val relationList = relationListBuffer.toList
      (shop, itemList, relationList)
    })

    //分别获取出店铺、商品、店铺与商品
    val shopDs:Dataset[Shop] = ds.map(one => {
      one._1
    })
    val itemDs: Dataset[Item] = ds.flatMap(one => {
      one._2
    })
    val relationDs: Dataset[Relation] = ds.flatMap(one => {
      one._3
    })

    //数据去重
    shopDs.createOrReplaceTempView("shop")
    itemDs.createOrReplaceTempView("item")
    relationDs.createOrReplaceTempView("relation")

    //写入hdfs 作为csv文件
    val date = getNowDate("yyyyMMdd")
    session.sql("select DISTINCT a.wm_poi_id,a.shop_name,a.create_time from shop a ").coalesce(4).write.option("header", "true").mode("Append").csv("hdfs://ELK01:9000/tmp/csv/"+date+"/shop.csv")
    session.sql("select DISTINCT b.spu_id,b.spu_name,b.create_time from item b ").coalesce(4).write.option("header", "true").mode("Append").csv("hdfs://ELK01:9000/tmp/csv/"+date+"/item.csv")
    session.sql("select DISTINCT c.wm_poi_id,c.shop_name,c.spu_id,c.spu_name,c.create_time from relation c ").coalesce(4).write.option("header", "true").mode("Append").csv("hdfs://ELK01:9000/tmp/csv/"+date+"/relation.csv")
  }

  def getNowDate(dataFormat:String):String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat(dataFormat)
    var time = dateFormat.format( now )
    time
  }

  case class Item(spu_id:Long, spu_name:String,create_time:String)

  case class Shop(wm_poi_id:String, shop_name:String,create_time:String)

  case class Relation(wm_poi_id:String, shop_name:String,spu_id:Long,spu_name:String,create_time:String)

}

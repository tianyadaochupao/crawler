package com.tang.crawler.flink


import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.catalog.hive.HiveCatalog


object UserBeheviorFlink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
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

    val  kafkaSource = new FlinkKafkaConsumer011[String]("user_behavior", new SimpleStringSchema(), properties)
    val inputStream = env.addSource( kafkaSource)
    val stream: DataStream[UserBehavior] = inputStream.map(new MapFunction[String,UserBehavior] {
      override def map(value: String): UserBehavior = {
        val jsonObject: JSONObject = JSON.parseObject(value)
        val sessionId = jsonObject.getString("sessionId")
        val userId = jsonObject.getString("userId")
        val mtWmPoiId = jsonObject.getString("mtWmPoiId")
        val shopName = jsonObject.getString("shopName")
        val platform = jsonObject.getString("platform")
        val source = jsonObject.getString("source")
        val createTime = jsonObject.getString("createTime")
        val dt = formatDateByFormat(createTime,"yyyy-MM-dd")
        val hr = formatDateByFormat(createTime,"HH")
        val mm = formatDateByFormat(createTime,"mm")
        UserBehavior(sessionId,userId,mtWmPoiId,shopName,platform,source,createTime,dt,hr,mm)
      }
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[UserBehavior](Duration.ofSeconds(20))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserBehavior] {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = {
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
    //流转为表
    tableEnv.createTemporaryView("user_behavior",stream)

      /*tableEnv.executeSql("""
                            |INSERT INTO ods_user_behavior_new
                            |SELECT session_id, user_id,mt_wm_poi_id,shop_name,source,platform,create_time,dt,hr,mm
                            |FROM user_behavior
                          """.stripMargin)*/

    tableEnv.executeSql("INSERT INTO ods_user_behavior_new SELECT session_id, user_id,mt_wm_poi_id,shop_name,source,platform,create_time,dt,hr,mm FROM user_behavior")
    print("执行成功")
    env.execute()
  }

  def formatDateByFormat (dateString:String,format: String ): String = {

    val sourceDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val date = sourceDateFormat.parse(dateString)
    val targetDateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val time = targetDateFormat.format(date)
    time
  }

  case class UserBehavior(session_id: String,user_id: String,mt_wm_poi_id: String,shop_name: String,source: String,platform: String,create_time:String,dt:String,hr:String,mm:String)
}
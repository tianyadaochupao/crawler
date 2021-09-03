package com.tang.crawler.flink.window


import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.time.Duration
import java.util._

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.catalog.hive.HiveCatalog

object UserBehaviorWindow {


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

    val  kafkaSource = new FlinkKafkaConsumer[String]("user_behavior", new SimpleStringSchema(), properties)
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
        UserBehavior(sessionId,userId,mtWmPoiId,shopName,platform,source,createTime,dt,hr,mm,1L)
      }
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[UserBehavior](Duration.ofSeconds(20))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserBehavior] {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        val cal =Calendar.getInstance();
        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = {
          val create_time = element.create_time
          val date = dateFormat.parse(create_time)
          // 对 eventTime做时区处理，不然后面使用eventTime的滑动窗口一直无法提交
          cal.setTime(date)
          cal.add(Calendar.HOUR_OF_DAY,-8)
          cal.getTime.getTime
        }
      }))
    // 窗口计算 使用翻滚窗口  2分钟翻滚一次
    val resultStream = stream.keyBy(_.mt_wm_poi_id)
      // 使用eventTime 翻滚窗口
      .window(TumblingEventTimeWindows.of(Time.minutes(2), Time.seconds(0)))
      .reduce((u1,u2)=>{
        val action_num =u1.action_num+u2.action_num
        UserBehavior(u1.session_id,u1.user_id,u1.mt_wm_poi_id,u1.shop_name,u1.platform,u1.source,u1.create_time,u1.dt,u1.hr,u1.mm,action_num)
      })
    val name            = "myHive"
    val defaultDatabase = "wm"
    val hiveConfDir     = "D:\\tang-spark2\\crawler\\src\\main\\resources"
    val version         = "2.3.4"
    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myHive", hive)
    tableEnv.useCatalog("myHive")
    tableEnv.useDatabase("wm")
    //流转为表
    tableEnv.createTemporaryView("hot_shop",resultStream)
    // ROW_NUMBER() OVER (PARTITION BY b.platform ORDER BY b.action_num DESC) as row_num
    //函数 按平台分组，分组内按店铺点击数倒序排序，取出分组内前n个数据即为平台Top-N热门店铺
    tableEnv.executeSql("insert into ods_top10_shop select REPLACE(UUID(),'-','') id,c.mt_wm_poi_id,c.shop_name,c.source,c.platform,c.time_key,c.action_num,'1' as status from (select b.mt_wm_poi_id,b.shop_name,b.source,b.platform,b.action_num,b.time_key,ROW_NUMBER() OVER (PARTITION BY b.platform ORDER BY b.action_num DESC) as row_num from ( select mt_wm_poi_id,shop_name,action_num,platform,source,dt||' '||hr||':'||mm time_key from hot_shop ) b ) c where c.row_num <=3 ")
    //sink
    //resultStream.addSink(new MyJdbcSink)
    print("启动成功")
    env.execute("hot shop execute .....")
  }

  def formatDateByFormat (dateString:String,format: String ): String = {

    val sourceDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val date = sourceDateFormat.parse(dateString)
    val targetDateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val time = targetDateFormat.format(date)
    time
  }

  case class UserBehavior(session_id: String,user_id: String,mt_wm_poi_id: String,shop_name: String,source: String,platform: String,create_time:String,dt:String,hr:String,mm:String,action_num:Long)


  /*class MyJdbcSink() extends RichSinkFunction[(UserBehavior)]{
    //连接、定义预编译器
    var conn:Connection = _
    var insertStmt:PreparedStatement = _
    var updateStmt:PreparedStatement = _
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    override def invoke(value: (UserBehavior), context: SinkFunction.Context): Unit = {
          //执行更新语句//执行更新语句

          val userBehavior = value
          val count = value.num
          updateStmt.setString(1, count + "")
          updateStmt.setString(2, userBehavior.dt +" "+ userBehavior.hr +":"+ userBehavior.mm)
          updateStmt.setString(3, dateFormat.format(new Date()))
          updateStmt.setString(4, userBehavior.mt_wm_poi_id)
          updateStmt.execute

          //如果update没有查到数据，执行插入语句
          if (updateStmt.getUpdateCount == 0) {
            insertStmt.setString(1, userBehavior.mt_wm_poi_id)
            insertStmt.setString(2, userBehavior.shop_name)
            insertStmt.setString(3, "")
            insertStmt.setString(4, "")
            insertStmt.setString(5, "hot-shop")
            insertStmt.setString(6, count + "")
            insertStmt.setString(7, userBehavior.dt +" "+ userBehavior.hr +":"+ userBehavior.mm)
            insertStmt.setString(8, dateFormat.format(new Date()))
            insertStmt.setString(9, dateFormat.format(new Date()))
            insertStmt.setString(10, "0")
            insertStmt.execute
          }
        }

    override def close(): Unit = {
      conn.close()
      insertStmt.close()
      updateStmt.close()
    }

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      conn = DriverManager.getConnection("jdbc:mysql://192.168.25.1:3306/wm","root","123456")
      insertStmt = conn.prepareStatement("insert into ods_hot_shop (mt_wm_poi_id,shop_name,source,platform,type,type_value,time_range,create_time,update_time,status) values (?,?,?,?,?,?,?,?,?,?)")
      updateStmt = conn.prepareStatement("update ods_hot_shop set type_value = ? ,time_range=?,update_time=? where mt_wm_poi_id = ?")
    }
  }*/
}

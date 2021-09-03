package com.tang.crawler.wb

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


object DbTableToKafkaSin {


  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val tableEnv = StreamTableEnvironment.create(env)
      val config = tableEnv.getConfig
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
      //1.source输入---kafka作为source
      //入参 topic SimpleStringSchema--读取kafka消息是string格式 properties kafka的配置

      val  kafkaSource = new FlinkKafkaConsumer[String]("wm_db", new SimpleStringSchema(), properties)
      import org.apache.flink.streaming.api.scala._
      val inputStream = env.addSource(kafkaSource)
      inputStream.print()
      // db中需要监控的表列表
      val list = List[String]("tm_key_users","tm_province")
      //定义list 只要list中的表的变动信息
      val  filterStream = inputStream.filter(new FilterFunction[String](){
        override def filter(value: String): Boolean ={
          val jsonObject: JSONObject = JSON.parseObject(value)
          if(null==jsonObject){
            return false;
          }
          val tableName = jsonObject.get("table")
          if(null==tableName){
            return false;
          }
          if(list.contains(tableName)){
            return true;
          }
          return false
        }
      })
      val p = new Properties()
      p.setProperty("bootstrap.servers", "ELK01:9092")
      p.setProperty("group.id", "consumer-group")
      p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      val kafkaProducerSin = new FlinkKafkaProducer[String](
      "",
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema() ){
        override def getTargetTopic(element: String): String ={
            val jsonObject: JSONObject = JSON.parseObject(element)
            val tableName = jsonObject.get("table")
            //默认为空则不覆盖原来的topic
            val topic:String="ods_"+tableName
            return topic;
          }
        },
        p,
       FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
    //sink到kafka
    filterStream.addSink(kafkaProducerSin)
    env.execute("db数据分发")
  }

}

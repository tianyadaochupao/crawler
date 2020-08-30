package com.tang.crawler.flink
import java.util.Date

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment



object ShopHiveFlink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //source 创建kafka streaming 表 接受数据
    val kafkaSql ="" +
      "SET table.sql-dialect=default;" +
      "CREATE TABLE shop_kafka (" +
      "sessionId VARCHAR," +
      "userId VARCHAR," +
      "mtWmPoiId VARCHAR," +
      "shopName VARCHAR," +
      "source STRING," +
      "platform STRING," +
      "createTime TIMESTAMP(3)" +
      ") WITH (" +
      "'connector' = 'kafka'," +
      "'topic' = 'shop_behavior'," +
      "'properties.bootstrap.servers' = '192.168.28.128:9092'," +
      "'properties.group.id' = 'user_behavior'," +
      "'format' = 'json'," +
      "'json.fail-on-missing-field' = 'false'," +
      "'json.ignore-parse-errors' = 'true'" +
      ");"


    env.execute()

    /* val schema = new Schema()
      .field("wm_poi_id", DataTypes.STRING())
      .field("dp_shop_id", DataTypes.BIGINT())
      .field("shop_name", DataTypes.STRING())
      .field("shop_status", DataTypes.BIGINT())
      .field("shop_pic", DataTypes.STRING())
      .field("delivery_fee", DataTypes.STRING())
      .field("delivery_type", DataTypes.STRING())
      .field("delivery_time", DataTypes.STRING())
      .field("min_fee", DataTypes.DOUBLE())
      .field("online_pay", DataTypes.BIGINT())
      .field("bulletin", DataTypes.STRING())
      .field("shipping_time", DataTypes.STRING())
      .field("create_time", DataTypes.STRING())
    val system = new FileSystem
    system.path("hdfs://ELK01:9000/user/hive/warehouse/wm.db/ods_shop_new")
    tableEnv.connect(system)
      .withFormat(new Csv().lineDelimiter("\\t"))
      .withSchema(schema)
      .createTemporaryTable("ods_shop_new")
*/
  }




  case class UserBehavior(sessionId: String,userId: String,mtWmPoiId: String,shopName: String,source: String,platform: String,createTime: Date)



}

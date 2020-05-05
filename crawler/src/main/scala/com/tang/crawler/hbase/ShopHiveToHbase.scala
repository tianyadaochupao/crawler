package com.tang.crawler.hbase


import java.text.SimpleDateFormat
import java.util.Date

import com.tang.crawler.utils.HbaseUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf}

object ShopHiveToHbase {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("shopHiveTohbase").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    val sc = sparkSession.sparkContext
    //调用工具类获取hbase配置
    val conf = HbaseUtils.getHbaseConfig()
    //hbase表
    val tableName = "wm:h_shop"
    val table = new HTable(conf, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date)
    val hfileTmpPath ="hdfs://ELK01:9000/tmp/hbase/output/"+currentTime;
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad (job, table)

    // 加载hivehdf中的数据
    sparkSession.sql("use wm")
    val hiveShopRdd = sparkSession.sql("select wm_poi_id ,shop_name ,shop_status,shop_pic from dws_shop where batch_date ='2020-05-03' ").rdd
    val hbaseRowRdd = hiveShopRdd.flatMap(rows=>{
      val wm_poi_id = rows(0).toString
      val shop_name = rows(1)
      val shop_status = rows(2)
      val shop_pic = rows(3)
      Array((wm_poi_id,(("info","wm_poi_id",wm_poi_id))),
        (wm_poi_id,(("info","shop_name",shop_name))),
        (wm_poi_id,(("info","shop_status",shop_status))),
        (wm_poi_id,(("info","shop_pic",shop_pic)))
      )
    })
    //过滤调空数据
    val rdd = hbaseRowRdd.filter(x=>x._1 != null).sortBy(x=>(x._1,x._2._1,x._2._2)).map(x=>{
      //将rdd转换成HFile需要的格式,Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
      //KeyValue的实例为value
      val rowKey = Bytes.toBytes(x._1)
      val family = Bytes.toBytes(x._2._1)
      val colum = Bytes.toBytes(x._2._2)
      val value = Bytes.toBytes(x._2._3.toString)
      (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, value))
    })

    // Save Hfiles on HDFS
    rdd.saveAsNewAPIHadoopFile(hfileTmpPath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)

    //Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(hfileTmpPath), table)
    sparkSession.stop()
  }

}

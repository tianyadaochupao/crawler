package com.tang.crawler.flink
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


object ResourcePathTest {

  def main(args: Array[String]): Unit = {

  /*  val resoucePath = ShopFlink.getClass.getClassLoader.getResource("hive-site.xml").getPath
    val parent = resoucePath.substring(0,resoucePath.lastIndexOf("/"))
    println(parent)*/
  val bsEnv = org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
  }

}

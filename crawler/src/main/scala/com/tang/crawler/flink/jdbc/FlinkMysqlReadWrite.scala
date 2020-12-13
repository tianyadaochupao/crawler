package com.tang.crawler.flink.jdbc

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}



/**
 * : flink 从mysql中读写数据
  *
  * @author tang
 *  2020/11/21 14:49
 */
object FlinkMysqlReadWrite {


  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = TableEnvironment.create(settings)

    val name            = "mysql"
    val defaultDatabase = "wm"
    val username        = "root"
    val password        = "123456"
    val baseUrl         = "jdbc:mysql://192.168.25.1:3306"

    val catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl)
    tableEnv.registerCatalog("mysql", catalog)
    tableEnv.useCatalog("mysql")
    
  }

}

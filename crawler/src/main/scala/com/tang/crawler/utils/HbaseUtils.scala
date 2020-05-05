package com.tang.crawler.utils


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration


object HbaseUtils {

  private var hbaseConfig: Configuration = _

  def getHbaseConfig(): Configuration = {
    if (hbaseConfig == null) {
      hbaseConfig = HBaseConfiguration.create()
      //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
      hbaseConfig.set("hbase.zookeeper.quorum","ELK01,ELK02,ELK03")
      //设置zookeeper连接端口，默认2181
      hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    }
    hbaseConfig
  }

}

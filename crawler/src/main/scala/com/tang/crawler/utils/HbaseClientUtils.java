package com.tang.crawler.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @Description: hbase工具类
 * @author tang
 * @date 2019/12/14 20:49
 */
public class HbaseClientUtils {

    private static HbaseClientUtils hbaseClient = new HbaseClientUtils();
    private static Configuration configuration = null;


    /**
     * @Description: 获取hbase的configuration对象
     * @author tang
     * @date 2019/12/14 20:59
     */
    public static Configuration getConfiguration(){

        if(null!=configuration){
            return configuration;
        }
        synchronized (hbaseClient){
             configuration = HBaseConfiguration.create();
            //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
            configuration.set("hbase.zookeeper.quorum","ELK01,ELK02,ELK03");
            //设置zookeeper连接端口，默认2181
            configuration.set("hbase.zookeeper.property.clientPort", "2181");

        }
        return configuration;

    }





}

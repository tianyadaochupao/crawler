package com.tang.crawler.hadoop;


import com.tang.crawler.utils.HbaseClientUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
    * @Description: Hfile文件数据加载进hbase
　　* @author tang
　　* @date 2019/12/14 20:31
　　*/
public class LoadHFileToHBaseUtils {

    public static void main(String[] args) throws Exception {

        /*String inputPath="hdfs://192.168.25.128:9000/tmp/hbase/output";*/
        String inputPath="hdfs://192.168.25.128:9000/tmp/hbase/output/20200505141559";
        String hbaseTable="wm:h_shop";
        Boolean load = load(inputPath, hbaseTable);
        System.out.println(load);
    }


    public static Boolean load(String inputPath,String hbaseTable) throws Exception{

        if(StringUtils.isBlank(inputPath)||StringUtils.isBlank(hbaseTable)){
            return false;
        }
        Configuration configuration = HbaseClientUtils.getConfiguration();
        HBaseConfiguration.addHbaseResources(configuration);
        LoadIncrementalHFiles loder = new LoadIncrementalHFiles(configuration);
        HTable hTable = new HTable(configuration, hbaseTable);
        loder.doBulkLoad(new Path(inputPath), hTable);
        return true;
    }

}

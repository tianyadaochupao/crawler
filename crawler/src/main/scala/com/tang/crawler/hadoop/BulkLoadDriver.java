package com.tang.crawler.hadoop;
import com.tang.crawler.utils.HbaseClientUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
    * @Description: 把hdfs上的文件转为hfile文件驱动类
　　* @author tang
　　* @date 2019/12/14 20:22
　　*/
public class BulkLoadDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //hdfs上txt数据
        final String SRC_PATH= "hdfs://192.168.25.128:9000/user/hive/warehouse/wm.db/dws_shop/batch_date=2020-05-03";
        //输出的hfile文件数据
        String currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        final String DESC_PATH ="hdfs://ELK01:9000/tmp/hbase/output/"+currentTime;
        String hbaseTable ="wm:h_shop";
        System.out.println(DESC_PATH);
        drive(SRC_PATH,DESC_PATH,hbaseTable);

    }

    /**
        * @Description: hdfs文件转换成hfile
    　　* @author tang
    　　* @date 2019/12/14 20:39
    　　*/
    public static void drive(String srcPath,String descPath,String hbaseTable) throws IOException, ClassNotFoundException, InterruptedException{

        if(StringUtils.isBlank(srcPath)||StringUtils.isBlank(descPath)||StringUtils.isBlank(hbaseTable)){
            throw  new RuntimeException("参数有误,有参数为空:"+"srcPath:"+srcPath+",descPath:"+descPath+",hbaseTable:"+hbaseTable);
        }

        Configuration conf = HbaseClientUtils.getConfiguration();

        Job job=Job.getInstance(conf);
        job.setJarByClass(BulkLoadDriver.class);
        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HTable table = new HTable(conf,hbaseTable);
        HFileOutputFormat2.configureIncrementalLoad(job,table,table.getRegionLocator());
        FileInputFormat.addInputPath(job,new Path(srcPath));
        FileOutputFormat.setOutputPath(job,new Path(descPath));
        //系统退出返回执行结果
        System.exit(job.waitForCompletion(true)?0:1);
    }

}

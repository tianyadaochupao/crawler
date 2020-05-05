package com.tang.crawler.hadoop;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
    * @Description: 转化为hbase数据的mapper
　　* @author tang
　　* @date 2019/12/14 20:19
　　*/
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split("\\t");

        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(items[0].getBytes());
        Put put = new Put(Bytes.toBytes(items[0]));   //ROWKEY
        put.addColumn("info".getBytes(), "wm_poi_id".getBytes(), items[0].getBytes());
        put.addColumn("info".getBytes(), "shop_name".getBytes(), items[2].getBytes());
        put.addColumn("info".getBytes(), "shop_status".getBytes(), items[3].getBytes());
        put.addColumn("info".getBytes(), "shop_pic".getBytes(), items[4].getBytes());
        context.write(rowKey, put);
    }
}

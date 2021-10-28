package com.tang.crawler.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerModel {

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put("bootstrap.servers", "ELK01:9092,ELK02:9092,ELK03:9092");
        // group.id，指定了消费者所属群组
        props.put("group.id", "CountryCounter");
        props.put("max.poll.records", "5");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // topic name is “customerCountries”
        consumer.subscribe(Collections.singletonList("test01"));
        try {
            int count = 0;
            List<String> list = new ArrayList<String>(50);
            while (true) {
                if(count>50){
                    break;
                }
                // 100 是超时时间（ms），在该时间内 poll 会等待服务器返回数据
                ConsumerRecords<String, String> records = consumer.poll(100);
                // poll 返回一个记录列表。
                // 每条记录都包含了记录所属主题的信息、记录所在分区的信息、记录在分区里的偏移量，以及记录的键值对。
                for (ConsumerRecord<String, String> record : records) {
                    if(count>50){
                        break;
                    }
                    System.out.println(String.format("topic=%s, partition=%s, offset=%d, customer=%s, country=%s",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), record.value()));
                    count++;
                    list.add(record.value());
                }
            }
            } finally {
                // 关闭消费者,网络连接和 socket 也会随之关闭，并立即触发一次再均衡
                consumer.close();
            }

    }
}

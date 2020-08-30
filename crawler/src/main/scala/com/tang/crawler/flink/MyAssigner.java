package com.tang.crawler.flink;


import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyAssigner implements AssignerWithPunctuatedWatermarks<UserBeheviorFlink.UserBehavior> {

    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    long water = 0l;

    @Nullable
    @Override
    public org.apache.flink.streaming.api.watermark.Watermark checkAndGetNextWatermark(UserBeheviorFlink.UserBehavior userBehavior, long l) {
         return new Watermark(water);
    }

    @Override
    public long extractTimestamp(UserBeheviorFlink.UserBehavior userBehavior, long l) {
        String time = userBehavior.create_time();
        Date date = null;
        try {
             date = dateFormat.parse(time);
        }catch (Exception e){
            date=new Date();
        }
        water = date.getTime();
        return water;
    }
}

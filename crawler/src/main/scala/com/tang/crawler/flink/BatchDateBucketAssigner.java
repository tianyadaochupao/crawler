package com.tang.crawler.flink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@PublicEvolving
public class BatchDateBucketAssigner<IN> implements BucketAssigner<IN, String> {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd";
    private final String formatString;
    private final ZoneId zoneId;
    private transient DateTimeFormatter dateTimeFormatter;

    public BatchDateBucketAssigner() {
        this("yyyy-MM-dd");
    }

    public BatchDateBucketAssigner(String formatString) {
        this(formatString, ZoneId.systemDefault());
    }

    public BatchDateBucketAssigner(ZoneId zoneId) {
        this("yyyy-MM-dd", zoneId);
    }

    public BatchDateBucketAssigner(String formatString, ZoneId zoneId) {
        this.formatString = (String) Preconditions.checkNotNull(formatString);
        this.zoneId = (ZoneId)Preconditions.checkNotNull(zoneId);
    }

    @Override
    public String getBucketId(IN in, Context context) {
        if (this.dateTimeFormatter == null) {
            this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(this.zoneId);
        }
        return "batch_date="+this.dateTimeFormatter.format(Instant.ofEpochMilli(context.timestamp()));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
         return SimpleVersionedStringSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "BatchDateBucketAssigner{" +
                "formatString='" + formatString + '\'' +
                ", zoneId=" + zoneId +
                ", dateTimeFormatter=" + dateTimeFormatter +
                '}';
    }
}

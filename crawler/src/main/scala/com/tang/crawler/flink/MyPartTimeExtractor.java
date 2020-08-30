package com.tang.crawler.flink;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.filesystem.PartitionTimeExtractor;

import javax.annotation.Nullable;
import java.time.*;
import java.time.format.*;
import java.time.temporal.ChronoField;
import java.util.List;

import static java.time.temporal.ChronoField.*;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;

public class MyPartTimeExtractor implements PartitionTimeExtractor {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendLiteral(" ")
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalEnd()
            .toFormatter()
            .withResolverStyle(ResolverStyle.LENIENT);

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .toFormatter()
            .withResolverStyle(ResolverStyle.LENIENT);

    @Nullable
    private  String pattern ="$dt $hr:$mm:00";

    public MyPartTimeExtractor(@Nullable String pattern) {
        if(null!=pattern){
            this.pattern = pattern;
        }
    }

    public MyPartTimeExtractor() {
    }

    @Override
    public LocalDateTime extract(List<String> partitionKeys, List<String> partitionValues) {
        String timestampString;
        if (pattern == null) {
            timestampString = partitionValues.get(0);
            System.out.println("时间戳:"+timestampString);
        } else {
            timestampString = pattern;
            for (int i = 0; i < partitionKeys.size(); i++) {
                timestampString = timestampString.replaceAll(
                        "\\$" + partitionKeys.get(i),
                        partitionValues.get(i));
            }
        }
        return toLocalDateTime(timestampString).plusHours(-8);
    }

    public static LocalDateTime toLocalDateTime(String timestampString) {
        try {
            return LocalDateTime.parse(timestampString, TIMESTAMP_FORMATTER);
        } catch (DateTimeParseException e) {
            return LocalDateTime.of(
                    LocalDate.parse(timestampString, DATE_FORMATTER),
                    LocalTime.MIDNIGHT);
        }
    }

    public static long toMills(LocalDateTime dateTime) {
        return TimestampData.fromLocalDateTime(dateTime).getMillisecond();
    }

    public static long toMills(String timestampString) {
        return toMills(toLocalDateTime(timestampString));
    }
}

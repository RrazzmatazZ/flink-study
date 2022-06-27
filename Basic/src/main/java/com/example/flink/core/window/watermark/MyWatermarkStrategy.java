package com.example.flink.core.window.watermark;

import com.example.flink.core.common.beans.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 有序流watermark生成
 */
public class MyWatermarkStrategy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                        new Event("Mary", "./home", 2000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Alice", "./prod?id=200", 3500L),
                        new Event("Bob", "./cart", 2500L),
                        new Event("Alice", "./cart", 3600L),
                        new Event("Bob", "./cart", 3000L),
                        new Event("Bob", "./cart", 2300L),
                        new Event("Bob", "./cart", 3300L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                );

        env.execute();
    }
}

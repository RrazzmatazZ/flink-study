package com.example.flink.core.window.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 通过WatermarkStrategy生成watermark
 */
public class MyWatermark01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("10.12.119.109", 4444);
        WatermarkStrategy<Map<String, Object>> strategy = WatermarkStrategy.<Map<String, Object>>forBoundedOutOfOrderness(
                Duration.ofSeconds(1)).withTimestampAssigner(new MyTimeAssigner("name"));
        source.map(new MapFunction<String, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> map(String value) throws Exception {
                        return null;
                    }
                })
                .assignTimestampsAndWatermarks(strategy);
    }

    public static class MyTimeAssigner implements SerializableTimestampAssigner<Map<String, Object>> {

        private final String timestampColumn;

        public MyTimeAssigner(String timestampColumn) {
            this.timestampColumn = timestampColumn;
        }

        /**
         * 提取数据里的timestamp字段为时间戳
         *
         * @param element         event
         * @param recordTimestamp element 的当前内部时间戳，或者如果没有分配时间戳，则是一个负数
         * @return The new timestamp.
         */
        @Override
        public long extractTimestamp(Map<String, Object> element, long recordTimestamp) {
            return Long.parseLong(element.get(this.timestampColumn).toString());
        }
    }
}

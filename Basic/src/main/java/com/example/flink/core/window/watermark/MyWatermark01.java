package com.example.flink.core.window.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 1.13版本通过WatermarkStrategy生成watermark
 * 输入
 * (HelloWaterMark,2,1553503185000)
 * (HelloWaterMark,2,1553503186000)
 * (HelloWaterMark,2,1553503187000)
 * (HelloWaterMark,2,1553503207000)
 * (HelloWaterMark,2,1553503209000)
 * 输出
 * 1> (HelloWaterMark,6,1553503185000)
 * 1> (HelloWaterMark,4,1553503207000)
 */
public class MyWatermark01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStreamSource<String> source = env.socketTextStream("10.12.109.183", 4444);
        WatermarkStrategy<Tuple3<String, Integer, Long>> strategy = WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(
                Duration.ofSeconds(5)).withTimestampAssigner(new MyTimeAssigner());
        source.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple3<>(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String, Integer, Long> value2) {
                        value1.f1 = value1.f1 + value2.f1;
                        return value1;
                    }
                })
                .print();

        env.execute();
    }

    public static class MyTimeAssigner implements SerializableTimestampAssigner<Tuple3<String, Integer, Long>> {
        /**
         * 提取数据里的timestamp字段为时间戳
         *
         * @param element         event
         * @param recordTimestamp element 的当前内部时间戳，或者如果没有分配时间戳，则是一个负数
         * @return The new timestamp.
         */
        @Override
        public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
            System.out.println(element);
            return element.f2;
        }
    }
}

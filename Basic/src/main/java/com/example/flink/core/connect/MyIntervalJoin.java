package com.example.flink.core.connect;

import com.example.flink.core.common.beans.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 间隔连接双流join
 */
public class MyIntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                Tuple2.of("Mary", 1000L),
                Tuple2.of("Alice", 1000L),
                Tuple2.of("Bob", 2000L),
                Tuple2.of("Alice", 2000L),
                Tuple2.of("Bob", 5100L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                })
        );

        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                new Event("Bob", "./cart", 2300L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./cart", 2500L),
                new Event("Alice", "./cart", 36000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./cart", 23000L),
                new Event("Bob", "./cart", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );

        orderStream.keyBy(data -> data.f0)
                .intervalJoin(eventStream.keyBy(Event::getUser))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, ProcessJoinFunction<Tuple2<String, Long>, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + "=>" + left);
                    }
                })
                .print();

        env.execute();
    }
}

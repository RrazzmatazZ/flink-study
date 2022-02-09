package com.example.flink.core.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * TODO eventTime Window水位线
 */
public class MyTumblingEventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("10.12.119.109", 4444);

        ds.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, LocalDateTime>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, Integer, LocalDateTime>> out) {
                        String[] s = value.split(" ");
                        for (String s1 : s) {
                            out.collect(new Tuple3<>(s1, 1, LocalDateTime.now()));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple3<String, Integer, LocalDateTime>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple3<String, Integer, LocalDateTime> element) {
                                return element.f2.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                            }
                        }
                )
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print("time tumbling window");

        env.execute();
    }
}

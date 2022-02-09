package com.example.flink.core.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 计数滚动窗口
 */
public class MyTumblingCountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("10.12.119.109", 4444);

        ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] s = value.split(" ");
                        for (String s1 : s) {
                            out.collect(new Tuple2<String, Integer>(s1, 1));
                        }
                    }
                }).keyBy(0)
                .countWindow(2)
                .sum(1)
                .print("count tumbling window");

        env.execute();
    }
}

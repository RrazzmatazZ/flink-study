package com.example.flink.core.window.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Window function:ReduceFunction(增量聚合)
 *
 * socket input example:test,ddd,9
 *
 */
public class MyReduceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("dev03", 4444);

        socket.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new Tuple3<>(arr[0], arr[1], Integer.parseInt(arr[2]));
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
                        System.out.printf("r1.value:%s,r2.value:%s%n", value1.f2, value2.f2);
                        if (value1.f2 > value2.f2) {
                            return value1;
                        } else {
                            return value2;
                        }
                    }
                })
                .print("MyReduceFunction");

        env.execute();

    }
}

package com.example.flink.core.connect;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class MyStreamConnect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Long> source2 = env.fromElements(1L, 2L, 3L, 4L, 5L);

        ConnectedStreams<Integer, Long> connect = source1.connect(source2);

        connect.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer" + value.toString();
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long" + value.toString();
            }
        }).print();


        env.execute();

    }
}

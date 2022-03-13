package com.example.flink.core.window.watermark;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;


/**
 * 1.13版本以下的api，有Period和Punctuated设定的版本
 */
public class MyWatermark02 {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream = env.socketTextStream("192.168.150.121", 4444)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    long currentTimeStamp = 0L;
                    //long maxDelayAllowed = 0L;
                    long maxDelayAllowed = 5000L; //这里设置5s的延迟

                    long currentWaterMark;

                    @Override
                    public Watermark getCurrentWatermark() {
                        currentWaterMark = currentTimeStamp - maxDelayAllowed;
                        return new Watermark(currentWaterMark);
                    }

                    @Override
                    public long extractTimestamp(String s, long l) {
                        String[] arr = s.split(",");
                        long timeStamp = Long.parseLong(arr[1]);
                        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                        System.out.println("Key:" + arr[0] + ",EventTime: " + sdf.format(timeStamp) + ",上一条数据的水位线: " + sdf.format(currentWaterMark));
                        return timeStamp;
                    }
                });

        dataStream
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        return new Tuple2<>(s.split(",")[0], s.split(",")[1]);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, String>, StringBuilder, String>() {
                    @Override
                    public StringBuilder createAccumulator() {
                        return new StringBuilder();
                    }

                    @Override
                    public StringBuilder add(Tuple2<String, String> value, StringBuilder accumulator) {
                        return accumulator.append("-").append(sdf.format(value.f1));
                    }

                    @Override
                    public String getResult(StringBuilder accumulator) {
                        return accumulator.toString();
                    }

                    @Override
                    public StringBuilder merge(StringBuilder a, StringBuilder b) {
                        return a.append("-").append(b);
                    }
                })
                .print();

        env.execute("WaterMark Test Demo");
    }
}

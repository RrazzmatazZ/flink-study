package com.example.flink.core.connect;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //app支付请求流
        DataStream<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order1", "app", 1000L),
                Tuple3.of("order2", "app", 2000L),
                Tuple3.of("order3", "app", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );
        //第三方支付平流
        DataStream<Tuple4<String, String, String, Long>> thirdPartStream = env.fromElements(
                Tuple4.of("order1", "third-part", "success", 3000L),
                Tuple4.of("order3", "third-part", "success", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        })
        );

        //检测同一支付单在两条流中是否匹配，不匹配就报警
        appStream.keyBy(data -> data.f0)
                .connect(thirdPartStream.keyBy(data -> data.f0))
                .process(new OrderMatchProcess())
                .print();

        env.execute();
    }

    private static class OrderMatchProcess extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        private ValueState<Tuple3<String, String, Long>> appState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartState;


        @Override
        public void open(Configuration parameters) throws Exception {
            appState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("last-app", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdPartState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("last-thirdPart", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));

        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (thirdPartState.value() != null) {
                out.collect("对账成功：" + value + " " + thirdPartState.value());
                thirdPartState.clear();
            } else {
                appState.update(value);
                //注册5秒的定时器等待支付
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (appState.value() != null) {
                out.collect("对账成功：" + appState.value() + " " + value);
                appState.clear();
            } else {
                thirdPartState.update(value);
                //这里等待时间可直接为支付时间，app的watermark的end_of_time不可能比支付的还慢
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            if (appState.value() != null) {
                out.collect("对账失败，第三方支付失败：" + appState.value());
            }
            if (thirdPartState.value() != null) {
                out.collect("对账失败，app未响应：" + thirdPartState.value());
            }
            appState.clear();
            thirdPartState.clear();
        }
    }
}

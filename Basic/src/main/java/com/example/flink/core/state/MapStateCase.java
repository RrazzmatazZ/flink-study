package com.example.flink.core.state;

import com.example.flink.core.common.ClickSource;
import com.example.flink.core.common.beans.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class MapStateCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 定义一个滚动窗口十秒钟，在这个10秒中之内的所有数据，按照url做一个划分，统计出每个url，每一个页面被点击的次数
         */
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {

                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                );

        stream.print("input");

        stream.keyBy(Event::getUrl)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();
    }

    //实现自定义的keyedProcessFunction
    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        //定义窗口大小
        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        //定义状态，用来保存每个窗口中统计的count值
        MapState<Long, Long> windowUrlCountMapState; //时间戳和个数

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取运行上下文
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，根据时间戳判断属于那个窗口（窗口分配器）
            Long windowStart = value.getTimestamp() / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            //注册 end - 1 的定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            //更新状态，进行增量聚合
            if (windowUrlCountMapState.contains(windowStart)) {
                Long count = windowUrlCountMapState.get(windowStart);
                windowUrlCountMapState.put(windowStart, count + 1);
            } else {
                windowUrlCountMapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发时输出计算结果
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;

            Long count = windowUrlCountMapState.get(windowStart);

            out.collect("窗口:" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                    + " url: " + ctx.getCurrentKey()
                    + " count: " + count
            );

            //模拟窗口的关闭，清空map状态中的值
            windowUrlCountMapState.remove(windowStart);
        }
    }
}
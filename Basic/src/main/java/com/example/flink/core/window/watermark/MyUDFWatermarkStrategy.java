package com.example.flink.core.window.watermark;

import com.example.flink.core.common.beans.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public class MyUDFWatermarkStrategy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "./home", 2000L),
                new Event("Bob", "./cart", 2300L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./cart", 2500L),
                new Event("Alice", "./cart", 3600L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./cart", 2300L),
                new Event("Bob", "./cart", 3300L)
        );

        eventDataStreamSource.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
            //watermark生成器
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new CustomWatermarkGenerator();
            }

            //提取时间戳的分配器
            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                };
            }
        });

        env.execute();
    }

    public static class CustomWatermarkGenerator implements WatermarkGenerator<Event> {

        //延迟时间
        private static Long delayTime = 5000L;

        //观察到的最大时间戳
        private static Long maxTs = Long.MIN_VALUE + delayTime + 1L;

        /**
         * 事件触发时执行
         *
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(eventTimestamp, maxTs);
        }

        /**
         * 周期性执行
         *
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //这里设置1ms延迟是为了形成左闭右开的场景
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}

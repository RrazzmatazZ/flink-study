package com.example.flink.core.process;

import com.example.flink.core.common.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

public class MyKeyedProcessFunction {

    public static void main(String[] args) {

    }

    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        ValueState<Long> tsTime;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-time", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, Integer>.Context ctx, Collector<Integer> out) throws Exception {

            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
            //注册eventTime的timer,因为eventTime从1970-01-01开始，所以要指定对应的开始时间
            ctx.timerService().registerEventTimeTimer((value.getDate() + 10) * 1000);

            tsTime.update(ctx.timerService().currentProcessingTime() + 1000L);
            //删除定时器
//            ctx.timerService().deleteProcessingTimeTimer(1000L);
        }

        /**
         * timer事件触发
         *
         * @param timestamp 当前触发的时间戳
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }

        @Override
        public void close() throws Exception {
            tsTime.clear();
        }
    }
}

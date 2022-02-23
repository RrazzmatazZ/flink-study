package com.example.flink.core.process;

import com.example.flink.core.common.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 监控温度传感器的温度值，如果温度值在10s内连续上升则触发报警
 */
public class TempIncreaseWarningCase {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.10.121", 4444);

        source.map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                    }
                }).keyBy(0)
                .process(new TempIncreaseWarning(10));

        env.execute();

    }


    public static class TempIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private final Integer interval;

        ValueState<Long> timerTsState; //定时器
        ValueState<Double> lastTempState; //上次的温度

        public TempIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            //先取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            //如果温度上升并且没有定时器，注册10s的定时器开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }//如果温度下降，那么删除定时器
            else if (value.getTemperature() < lastTemp && timerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            //更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //只要触发定时器一定是连续上升
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续上升");
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
            timerTsState.clear();
        }
    }
}

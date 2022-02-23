package com.example.flink.core.state;

import com.example.flink.core.common.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过keyedState来进行温度跳变的预警
 */
public class KeyedStateCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.150.121", 4444);

        SingleOutputStreamOperator<SensorReading> dataStream = source.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
            }
        });

        dataStream.keyBy(SensorReading::getName)
                .flatMap(new TempChangeWarning(10.0))
                .print();


        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {


        private final Double threshold;

        //定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;


        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            //如果状态不为null，那么就判断两次的温度差值
            if (lastTemp != null) {
                double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff >= threshold) {
                    out.collect(new Tuple3<>(value.getName(), lastTemp, value.getTemperature()));
                }
            }
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}

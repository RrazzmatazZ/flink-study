package com.example.flink.core.transform;

import com.example.flink.core.common.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 基础的转换算子 filter map flatmap
 */
public class BasicTransform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> sensorDataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStream<Integer> mapStream = sensorDataStream.map(new MapFunction<SensorReading, Integer>() {
            public Integer map(SensorReading sensorReading) throws Exception {
                return sensorReading.getName().length();
            }
        });

        DataStream<SensorReading> filterStream = sensorDataStream.filter(new FilterFunction<SensorReading>() {
            public boolean filter(SensorReading sensorReading) throws Exception {
                return sensorReading.getTemperature() > 20;
            }
        });

        mapStream.print("map");
        filterStream.print("filter");

        env.execute();
    }
}

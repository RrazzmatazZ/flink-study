package com.example.flink.core.transform;

import com.example.flink.core.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 多流结合 union操作，但是多条流合并的数据格式必须相同
 */
public class UnionTransform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //高温流
        DataStream<SensorReading> highSensorDataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));
        //低温流
        DataStream<SensorReading> lowSensorDataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7)
        ));

        DataStream<SensorReading> union = highSensorDataStream.union(lowSensorDataStream, lowSensorDataStream);

        union.print();
        env.execute();
    }
}

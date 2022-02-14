package com.example.flink.core.transform;

import com.example.flink.core.common.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * 流连接合并处理
 */
public class ConnectedTransform {

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

        ConnectedStreams<SensorReading, SensorReading> connect = highSensorDataStream.connect(lowSensorDataStream);

        /**
         * 这里也可以输出不相同格式的数据，对应function的名称都加上了co 比如coMap，coFlatmap
         */
        SingleOutputStreamOperator<Object> map = connect.map(new CoMapFunction<SensorReading, SensorReading, Object>() {
            @Override
            public Object map1(SensorReading value) throws Exception {
                return new Tuple3<>(value.getName(), value.getTemperature(), "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple3<>(value.getName(), value.getTemperature(), "low temp");
            }
        });

        map.print();

        env.execute();

    }
}

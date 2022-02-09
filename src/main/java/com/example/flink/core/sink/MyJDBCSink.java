package com.example.flink.core.sink;

import com.example.flink.core.source.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;

public class MyJDBCSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        DataStream<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));
        source.addSink(new MySQLSinkFunction());
        env.execute();
    }

    public static class MySQLSinkFunction extends RichSinkFunction<SensorReading> {

        PreparedStatement insertStat;
        PreparedStatement updateStat;
        private Connection conn;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (conn == null) {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            }
            // 创建预编译器，有占位符，可传入参数
            insertStat = conn.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES (?, ?)");
            updateStat = conn.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            super.invoke(value, context);
            updateStat.setString(1, value.getName());
            updateStat.setDouble(2, value.getTemperature());
            updateStat.execute();
            if (updateStat.getUpdateCount() == 0) {
                insertStat.setString(1, value.getName());
                insertStat.setDouble(2, value.getTemperature());
                insertStat.execute();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            insertStat.close();
            updateStat.close();
            conn.close();
        }
    }
}

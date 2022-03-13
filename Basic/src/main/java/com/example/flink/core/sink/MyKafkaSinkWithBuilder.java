package com.example.flink.core.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class MyKafkaSinkWithBuilder {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.12.109.183:9092");
        DataStreamSource<String> source = env.fromElements("1", "2", "3", "4");
    }
}

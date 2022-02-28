package com.example.flink.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

/**
 * File Connector的方式，这里需要引用csv的format依赖
 */
public class MyFileConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sql =
                "create table FileSource(" +
                        "name STRING," +
                        "timestamps BIGINT," +
                        "temp DOUBLE" +
                        ") WITH(" +
                        "'connector'='filesystem'," +
                        "'path'='F:\\code\\github\\flink-study\\src\\main\\resources\\sensor.txt'," +
                        "'format' ='csv')";
        tableEnvironment.executeSql(sql);

        Table fileSource = tableEnvironment.from("FileSource");
        fileSource.printSchema();//打印表结构
        tableEnvironment.toAppendStream(fileSource, Row.class).print("source");
        Table filter = fileSource.select($("*"))
                .filter($("name").isEqual("sensor_1"));
        tableEnvironment.toAppendStream(filter, Row.class).print("filtered");
        env.execute();
    }
}

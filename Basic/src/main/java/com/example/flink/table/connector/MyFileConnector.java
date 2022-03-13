package com.example.flink.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.GroupedTable;
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
        //输入
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

        //操作
        Table fileSource = tableEnvironment.from("FileSource");
        fileSource.printSchema();//打印表结构
        tableEnvironment.toAppendStream(fileSource, Row.class).print("source");
        Table filter = fileSource.select($("*"))
                .filter($("name").isEqual("sensor_1"));
        tableEnvironment.toAppendStream(filter, Row.class).print("filtered");
        Table agg = fileSource
                .groupBy($("name"))
                .select($("temp").avg().as("avgTemp"), $("name"))
                .filter($("name").isEqual("sensor_1"));
        tableEnvironment.toRetractStream(agg, Row.class).print("agg");

        //输出
        String sinkSql =
                "create TEMPORARY table FileSink(" +
                        "name STRING," +
                        "timestamps BIGINT," +
                        "temp DOUBLE" +
                        ") WITH(" +
                        "'connector'='filesystem'," +
                        "'path'='E:\\output'," +
                        "'format' ='json')";
        tableEnvironment.executeSql(sinkSql);
        //这里不能输出agg这种聚合，因为是Retract类型，但是输出到file都是append的形式
        filter.executeInsert("FileSink");


        env.execute();
    }
}

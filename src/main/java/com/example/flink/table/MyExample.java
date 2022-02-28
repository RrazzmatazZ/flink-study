package com.example.flink.table;

import com.example.flink.core.common.beans.SensorReading;
import com.example.flink.wordcount.WordCountStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用示例
 */
public class MyExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //新版本必须在这里声明，因为新版的Env用的是blinkPlanner
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        DataStream<SensorReading> map = env.readTextFile(WordCountStream.class.getResource("/sensor.txt").getPath()).map(str -> {
            String[] split = str.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        //创建表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        Table table = tableEnvironment.fromDataStream(map, $("name"), $("temperature"));

        //DSL写法
        Table tableDSL = table.select($("temperature"), $("name"))
                .where($("name").isEqual("sensor_1"));
        tableEnvironment.toAppendStream(tableDSL, Row.class).print("dsl");

        //SQL写法
        tableEnvironment.createTemporaryView("sensor", table);
        Table tableSQL = tableEnvironment.sqlQuery("select name,temperature from sensor where name='sensor_1'");
        tableEnvironment.toAppendStream(tableSQL, Row.class).print("sql");

        env.execute();
    }
}

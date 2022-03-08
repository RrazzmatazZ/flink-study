package com.example.flink.table.function.time;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.types.Row;

import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class MyTableEventTime {
    public static void main(String[] args) {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        String sourceSql =
                "create table FileSource(" +
                        "name STRING," +
                        "ts BIGINT," +
                        "temp DOUBLE," +
                        "rt AS TO_TIMESTAMP(FROM_UNIXTIME(ts)), " +
                        "watermark for rt as rt - INTERVAL '1' SECOND" +
                        ") WITH(" +
                        "'connector'='filesystem'," +
                        "'format'='csv'," +
                        "'path'='G:\\github\\flink-study\\src\\main\\resources\\sensor.txt')";

        tableEnvironment.executeSql(sourceSql);

        Table fileSource = tableEnvironment.from("FileSource").select($("*"));

        Table select = fileSource.window(Tumble.over(lit(10).minutes()).on($("rt")).as("w"))
                .groupBy($("name"), $("w"))
                .select($("name"), $("w").end().as("w_end"), $("temp").max().as("max_temp"));

        String sinkSQL =
                "create table PrintSink(" +
                        "name STRING," +
                        "w_end TIMESTAMP_LTZ(3), " +
                        "max_temp DOUBLE" +
                        ") WITH(" +
                        "'connector'='print')";
        tableEnvironment.executeSql(sinkSQL);
        
        select.executeInsert("PrintSink");
    }
}

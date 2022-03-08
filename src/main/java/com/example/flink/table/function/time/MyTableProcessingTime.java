package com.example.flink.table.function.time;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

/**
 * 从table sql中读取processingTime
 */
public class MyTableProcessingTime {

    public static void main(String[] args) throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        String sourceSQL =
                "create table FileSource(" +
                        "name STRING," +
                        "timestamps BIGINT," +
                        "temp DOUBLE," +
                        "pt AS PROCTIME() " +
                        ") WITH(" +
                        "'connector'='filesystem'," +
                        "'format'='csv'," +
                        "'path'='G:\\github\\flink-study\\src\\main\\resources\\sensor.txt')";
        tableEnvironment.executeSql(sourceSQL);

        String sinkSQL =
                "create table PrintSink(" +
                        "name STRING," +
                        "timestamps BIGINT," +
                        "temp DOUBLE," +
                        "pt TIMESTAMP_LTZ(3) " +
                        ") WITH(" +
                        "'connector'='print')";
        tableEnvironment.executeSql(sinkSQL);

        String ptSql = "select * from FileSource";
        Table ptTable = tableEnvironment.sqlQuery(ptSql);
        ptTable.printSchema();
        ptTable.executeInsert("PrintSink");


    }
}

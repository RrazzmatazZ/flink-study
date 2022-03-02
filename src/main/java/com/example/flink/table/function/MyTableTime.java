package com.example.flink.table.function;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.TimeZone;

/**
 * 从table sql中读取processingTime和eventTime
 */
public class MyTableTime {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
//        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
//        String sql =
//                "create table FileSource(" +
//                        "name STRING," +
//                        "timestamps BIGINT," +
//                        "temp DOUBLE," +
//                        "pt AS PROCTIME() " +
//                        ") WITH(" +
//                        "'connector'='filesystem'," +
//                        "'path'='G:\\github\\flink-study\\src\\main\\resources\\sensor.txt'," +
//                        "'format' ='csv')";
//        tableEnvironment.executeSql(sql);
//        System.out.println(Arrays.toString(tableEnvironment.listTables()));
//
//        String ptSql = "select * from FileSource";
//        Table ptTable = tableEnvironment.sqlQuery(ptSql);
//        ptTable.printSchema();
//        tableEnvironment.toAppendStream(ptTable, Row.class).print();
//
//        String proc = "SELECT PROCTIME()";
//        Table procTb = tableEnvironment.sqlQuery(proc);
//        tableEnvironment.toAppendStream(procTb, Row.class).print();
//
//        executionEnvironment.execute();

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

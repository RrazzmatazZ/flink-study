package com.example.flink.table.function.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义TableFunction
 */
public class MyTableFunction {

    public static void main(String[] args) {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        String sourceSQL =
                "create table FileSource(" +
                        "name STRING," +
                        "timestamps BIGINT," +
                        "temp DOUBLE" +
                        ") WITH(" +
                        "'connector'='filesystem'," +
                        "'format'='csv'," +
                        "'path'='F:\\code\\github\\flink-study\\src\\main\\resources\\sensor.txt')";
        tableEnvironment.executeSql(sourceSQL);

        String sinkSQL =
                "create table PrintSink(" +
                        "name STRING," +
                        "word STRING," +
                        "len BIGINT" +
                        ") WITH(" +
                        "'connector'='print')";
        tableEnvironment.executeSql(sinkSQL);

        tableEnvironment.createTemporarySystemFunction("split", new Split("_"));
        //这里的输出为一条数据对于两条[sensor_10, sensor, 6(sensor的字符长度)][sensor_10, 10, 2(10的字符长度)]
        Table select = tableEnvironment
                .from("FileSource")
                .leftOuterJoinLateral(call("split", $("name")).as("word", "len"))
                .select($("name"), $("word"), $("len"));
//        select.executeInsert("PrintSink");


        Table tableSql1 = tableEnvironment.sqlQuery(
                "SELECT name, word, len " +
                        "FROM FileSource, LATERAL TABLE(split(name))");
        tableSql1.executeInsert("PrintSink");

        Table tableSql2 = tableEnvironment.sqlQuery(
                "SELECT name, word, len " +
                        "FROM FileSource " +
                        "LEFT JOIN LATERAL TABLE(split(name)) AS T(word, len) ON TRUE");
//        tableSql2.executeInsert("PrintSink");



    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING, len INT>"))
    public static class Split extends TableFunction<Row> {
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(Row.of(s, s.length()));
            }
        }
    }
}

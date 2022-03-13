package com.example.flink.table.function.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class MyUDAFFunction {
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
                        "avgTemp DOUBLE" +
                        ") WITH(" +
                        "'connector'='print')";
        tableEnvironment.executeSql(sinkSQL);

        tableEnvironment.createTemporarySystemFunction("WeightedAvg", MyAggFunction.class);

        Table select = tableEnvironment.from("FileSource")
                .groupBy($("name"))
                .select($("name"), call("WeightedAvg", $("temp")).as("avgTemp"));
        select.executeInsert("PrintSink");



        Table selectSql = tableEnvironment.sqlQuery(
                "SELECT name, WeightedAvg(temp) as avgTemp FROM FileSource GROUP BY name"
        );
        selectSql.executeInsert("PrintSink");
    }

    /**
     * 求温度的平均值，acc为Tuple2，温度总和和温度总个数
     */
    public static class MyAggFunction extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> acc) {
            return acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        //这个方法必须实现，并且名称固定，和eval类似
        public void accumulate(Tuple2<Double, Integer> acc, Double input) {
            acc.f0 += input;
            acc.f1++;
        }
    }
}

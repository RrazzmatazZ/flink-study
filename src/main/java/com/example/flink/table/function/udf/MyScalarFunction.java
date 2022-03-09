package com.example.flink.table.function.udf;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义标量函数
 */
public class MyScalarFunction {
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
                        "hash BIGINT" +
                        ") WITH(" +
                        "'connector'='print')";
        tableEnvironment.executeSql(sinkSQL);

        //在环境中注册UDF后调用
        tableEnvironment.createTemporaryFunction("hashCode", new HashCode(12));
        Table selectRegistered = tableEnvironment.from("FileSource").select($("name"), call("hashCode", $("name")).as("hash"));
        selectRegistered.executeInsert("PrintSink");
        Table selectRegisteredSQL = tableEnvironment.sqlQuery("SELECT name,hashcode(name) from FileSource");
        selectRegisteredSQL.executeInsert("PrintSink");


        //不经注册直接调用
        Table selectNotRegistered = tableEnvironment.from("FileSource").select($("name"), call(new HashCode(14), $("name")).as("hash"));
        selectNotRegistered.executeInsert("PrintSink");
    }

    /**
     * 自定义标量函数实现求id的hash值
     */
    public static class HashCode extends ScalarFunction {

        private final int factor;

        public HashCode(int factor) {
            this.factor = factor;
        }

        /**
         * 这个public方法必须申明，方法名称固定，参数和返回值不固定
         */
        public int eval(String str) {
            return str.hashCode() + factor;
        }
    }
}

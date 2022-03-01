package com.example.flink.table.catalogs;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * The GenericInMemoryCatalog is an in-memory implementation of a catalog. All objects will be available only for the lifetime of the session.
 */
public class MyInMemoryCatalog {

    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String sql =
                "create table FileSource(" +
                        "name STRING," +
                        "timestamps BIGINT," +
                        "temp DOUBLE" +
                        ") WITH(" +
                        "'connector'='filesystem'," +
                        "'path'='F:\\code\\github\\flink-study\\src\\main\\resources\\sensor.txt'," +
                        "'format' ='csv')";
        tableEnv.executeSql(sql);
        String[] databases = tableEnv.listDatabases();
        //这里输出的是default_database
        System.out.println(Arrays.toString(databases));
        String[] strings = tableEnv.listTables();
        System.out.println(Arrays.toString(strings));
    }
}

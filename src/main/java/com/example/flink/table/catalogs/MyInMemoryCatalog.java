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
        String sql = "CREATE TABLE blackhole_table (" +
                "  f0 INT," +
                "  f1 INT," +
                "  f2 STRING," +
                "  f3 DOUBLE" +
                ") WITH (" +
                "  'connector' = 'blackhole'" +
                ")";
        tableEnv.executeSql(sql);
        String[] databases = tableEnv.listDatabases();
        //这里输出的是default_database
        System.out.println(Arrays.toString(databases));
        String[] strings = tableEnv.listTables();
        System.out.println(Arrays.toString(strings));
    }
}

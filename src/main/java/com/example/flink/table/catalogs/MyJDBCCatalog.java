package com.example.flink.table.catalogs;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import java.util.List;

/**
 * flink 1.13版本只支持pg库，1.15开始支持mysql
 * <p>
 * 坑：1.13的pg只支持部分方法，如果需要执行添加表，需要升级到1.15
 */
@Deprecated
public class MyJDBCCatalog {
    public static void main(String[] args) throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        Catalog catalog = new JdbcCatalog(
                "pg_catalog",
                "flink",
                "postgres",
                "abc123",
                "jdbc:postgresql://10.12.109.248:55433");
        tableEnv.registerCatalog("pg_catalog", catalog);
        tableEnv.useCatalog("pg_catalog");
        tableEnv.useDatabase("flink");

        String sql = "CREATE TABLE blackhole_table (" +
                "  f0 INT," +
                "  f1 INT," +
                "  f2 STRING," +
                "  f3 DOUBLE" +
                ") WITH (" +
                "  'connector' = 'blackhole'" +
                ")";
        tableEnv.executeSql(sql);

        List<String> list = catalog.listDatabases();
        System.out.println(list);

        //这里会报UnsupportedOperationException，因为目前版本还不支持该方法
        List<String> tables = catalog.listTables("flink");
        System.out.println(tables);

        Table fileSource = tableEnv.from("blackhole_table");
        fileSource.printSchema();//打印表结构
    }
}

package com.example.flink.project.HotItemsAnalyze;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class HotItemsWithSQL {

    public static void main(String[] args) {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        
    }
}

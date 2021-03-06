package com.example.flink.core.common.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String name;
    private Long date;
    private Double temperature;
}
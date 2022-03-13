package com.example.flink.core.common.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Grade {
    private String name;
    private Integer grade;
    private String sex;
}

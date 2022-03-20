package com.example.flink.project.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdCountViewByProvince {
    private String province;
    private Long windowEnd;
    private Long count;
}

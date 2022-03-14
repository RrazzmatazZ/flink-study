package com.example.flink.project.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}

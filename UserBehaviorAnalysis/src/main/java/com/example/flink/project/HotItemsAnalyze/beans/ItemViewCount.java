package com.example.flink.project.HotItemsAnalyze.beans;

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

package com.example.flink.project.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 页面浏览统计
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long count;
}

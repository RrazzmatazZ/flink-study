package com.example.flink.project.NetworkFlowAnalysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long timestamp;
    private String method;
    private String url;
}

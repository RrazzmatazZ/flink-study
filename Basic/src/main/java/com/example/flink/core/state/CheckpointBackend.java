package com.example.flink.core.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 检查点相关
 */
public class CheckpointBackend {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(300);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint保存的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        //同时进行的checkpoint个数
        //默认情况下一个作业只允许1个Checkpoint执行，如果某个Checkpoint正在进行，另外一个Checkpoint被启动，新的Checkpoint需要挂起等待。
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //最小暂歇时间（前一个checkpoint保存完成到下一个checkpoint开始保存之间的时间间隔）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        //允许的checkpoint保存失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);


        //#region 重启策略的配置
        //固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L)); //每隔10s重启一次，尝试3次
        //失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(1), Time.seconds(10))); //每隔10s重启一次，尝试3次,每次重启时间不能超过1s


        //#endregion
    }
}

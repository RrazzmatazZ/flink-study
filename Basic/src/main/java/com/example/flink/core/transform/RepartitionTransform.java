package com.example.flink.core.transform;

import com.example.flink.core.common.beans.SensorReading;
import com.example.flink.wordcount.WordCountStream;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;

public class RepartitionTransform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorDataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        //1.随机分区
        sensorDataStream.shuffle().print().setParallelism(4);

        //2.轮询分区
        sensorDataStream.rebalance().print().setParallelism(4);

        //3.rescale重缩放分区
        //上层整除后均分到下层
        //意义在于比如有2个TaskManager，每个manager有2个slot，上层分别在两个TM上，下层就可以直接分在对应的TM下的slot执行，减少网络传输损耗
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            if ((i % 2) == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                })
                .setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);

        //4.广播方式
        sensorDataStream.broadcast().print().setParallelism(4);

        //5.全局分区，将记录输出到下游的第一个分区
        sensorDataStream.global().print().setParallelism(4);

        //6.forward,要求上下并行度一样，直接输出到下游本地的分区
        sensorDataStream.forward().print().setParallelism(1);

        //7.自定义分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print()
                .setParallelism(2);

        env.execute();
    }
}

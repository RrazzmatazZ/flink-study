package com.example.flink.core.transform;

import com.example.flink.core.common.beans.SensorReading;
import com.example.flink.wordcount.WordCountStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * split和select算子，用于多流转换，flink 1.7+版本已经被side output替代
 */
public class SplitSelectTransform {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStream<String> ds = env.readTextFile(WordCountStream.class.getResource("/sensor.txt").getPath());
        DataStream<SensorReading> map = ds.map(str -> {
            String[] split = str.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        map.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {

            }
        });
    }
}

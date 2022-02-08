package com.example.flink.core.transform;

import com.example.flink.core.source.SensorReading;
import com.example.flink.wordcount.WordCountStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分组聚合
 */
public class AggTransform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStream<String> ds = env.readTextFile(WordCountStream.class.getResource("/sensor.txt").getPath());
        DataStream<SensorReading> map = ds.map(str -> {
            String[] split = str.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        KeyedStream<SensorReading, String> keyedStream = map.keyBy(SensorReading::getName);
        //max和maxBy有区别，max返回流的最大值，maxBy返回的是对应最大值的键
        SingleOutputStreamOperator<SensorReading> maxBy = keyedStream.maxBy("temperature");
        SingleOutputStreamOperator<SensorReading> max = keyedStream.max("temperature");

        //这里的reduce操作在保持temperature字段最大值同时更新对应的date字段，这个操作max和maxBy无法做到
        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce((pre, cur) ->
                new SensorReading(cur.getName(), cur.getDate(), Math.max(pre.getTemperature(), cur.getTemperature()))
        );

        maxBy.print("maxBy");
        max.print("max");
        reduce.print("reduce");

        env.execute();
    }
}

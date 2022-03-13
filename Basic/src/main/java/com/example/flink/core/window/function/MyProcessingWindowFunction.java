package com.example.flink.core.window.function;

import com.example.flink.core.common.beans.Grade;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * processing window function(全窗口遍历执行)
 */
public class MyProcessingWindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("10.12.119.109", 4444);

        source.map(new MapFunction<String, Grade>() {
                    @Override
                    public Grade map(String value) throws Exception {
                        String[] val = value.split(",");
                        return new Grade(val[0], Integer.valueOf(val[1]), val[2]);
                    }
                })
                .keyBy(Grade::getGrade)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<Grade, Tuple2<Long, Long>, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Grade, Tuple2<Long, Long>, Integer, TimeWindow>.Context context,
                                        Iterable<Grade> elements, Collector<Tuple2<Long, Long>> out) throws Exception {
                        long females = 0L;
                        long males = 0L;
                        for (Grade element : elements) {
                            if (element.getSex().equals("male")) {
                                males += 1;
                            } else if (element.getSex().equals("female")) {
                                females += 1;
                            }
                        }
                        out.collect(new Tuple2<Long, Long>(males, females));
                    }
                })
                .print();

        env.execute();
    }
}
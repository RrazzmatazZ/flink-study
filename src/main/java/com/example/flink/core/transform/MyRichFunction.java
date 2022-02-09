package com.example.flink.core.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

public class MyRichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> map = integerDataStreamSource.map(new RichMapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask() + 1;
                return new Tuple2<>(indexOfThisSubtask, integer);
            }
        });
        map.print();
        env.execute();
    }
}

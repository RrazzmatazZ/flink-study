package com.example.flink.core.window.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class MyAggWithProcessFunction {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("dev03", 4444);
        socket
                .map((MapFunction<String, Record>) value -> {
                    String[] arr = value.split(",");
                    return new Record(arr[0], arr[1], Integer.parseInt(arr[2]));
                })
                .keyBy(Record::getKey)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .aggregate(new AvgAggregateFunction(), new AvgProcessFunction())
                .print("MyAggregateFunction");

        env.execute();
    }

    public static class AvgAggregateFunction implements AggregateFunction<Record, MyAvgAccumulator, Long> {
        @Override
        public MyAvgAccumulator createAccumulator() {
            return new MyAvgAccumulator();
        }

        @Override
        public MyAvgAccumulator add(Record value, MyAvgAccumulator accumulator) {
            System.out.printf("input value:%s%n", value);
            accumulator.records.add(value);
            accumulator.count += 1;
            accumulator.sum += value.count;
            return accumulator;
        }

        @Override
        public Long getResult(MyAvgAccumulator accumulator) {
            return accumulator.sum / accumulator.count;
        }

        @Override
        public MyAvgAccumulator merge(MyAvgAccumulator a, MyAvgAccumulator b) {
            a.records.addAll(b.records);
            a.sum += b.sum;
            a.count += b.count;
            return a;
        }
    }

    public static class AvgProcessFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        /**
         * @param s        key值
         * @param context  上下文
         * @param elements 这里的element对应的是agg的输出
         * @param out      输出内容
         * @throws Exception
         */
        @Override
        public void process(String s, ProcessWindowFunction<Long, String, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long avg = elements.iterator().next();
            out.collect(String.format("窗口 %s - %s 的avg值为：%s", start, end, avg));
        }
    }

    @Data
    @AllArgsConstructor
    static class Record {
        private String key;
        private String value;
        private Integer count;
    }

    @Data
    @AllArgsConstructor
    static class MyAvgAccumulator {
        private List<Record> records;
        private Long count;
        private Long sum;

        public MyAvgAccumulator() {
            this.records = new ArrayList<>();
            this.sum = 0L;
            this.count = 0L;
        }
    }


}

package com.example.flink.core.window.function;

import com.example.flink.core.common.beans.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 自定义trigger
 */
public class MyTriggerFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("dev03", 4444);

        WindowedStream<Tuple2<Integer, Integer>, Tuple, TimeWindow> trigger = socket
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(String value) throws Exception {
                        return new Tuple2<>(Integer.parseInt(value), 1);
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .trigger(new MyTrigger());

        trigger.sum(1).print();

        env.execute();
    }

    public static class MyTrigger extends Trigger<Tuple2<Integer, Integer>, TimeWindow> {

        //在有元素进入的时候进行的操作
        @Override
        public TriggerResult onElement(Tuple2<Integer, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        //在ProcessTime执行
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        //在EventTime执行
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        }
    }
}



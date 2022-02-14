package com.example.flink.core.window.function;

import com.example.flink.core.common.beans.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *
 */
public class MyTriggerFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("dev03", 4444);

        WindowedStream<Person, String, TimeWindow> trigger = socket.map(new MapFunction<String, Person>() {
                    @Override
                    public Person map(String value) throws Exception {
                        return null;
                    }
                })
                .keyBy(Person::getSex)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .trigger(new MyTrigger());


        env.execute();
    }

    public static class MyTrigger extends Trigger<Person, TimeWindow> {

        @Override
        public TriggerResult onElement(Person element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }
}



package com.example.flink.core.window.function;

import com.example.flink.core.common.beans.Person;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * 侧输出流
 */
public class MySideOutputFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> source = env.fromCollection(Arrays.asList(
                new Person("zhangsan", 12, "male"),
                new Person("lisi", 12, "female"),
                new Person("wangwu", 12, "male"),
                new Person("zhaoliu", 12, "female"),
                new Person("chenqi", 12, "male")
        ));

        OutputTag<Person> female = new OutputTag<Person>("female"){};

        SingleOutputStreamOperator<Person> male = source.process(new ProcessFunction<Person, Person>() {
            @Override
            public void processElement(Person value, ProcessFunction<Person, Person>.Context ctx, Collector<Person> out) throws Exception {
                if (value.getSex().equals("male")) {
                    out.collect(value);
                } else {
                    ctx.output(female, value);
                }
            }
        });
        DataStream<Person> sideOutput = male.getSideOutput(female);
        sideOutput.print("female");

        env.execute();
    }
}


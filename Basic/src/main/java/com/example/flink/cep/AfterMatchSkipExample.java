package com.example.flink.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class AfterMatchSkipExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> source = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "a1", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "a2", 3000L),
                new LoginEvent("user_1", "171.56.23.10", "a3", 5000L),
                new LoginEvent("user_1", "192.168.1.29", "b1", 8000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        //默认模式为no_skip策略
        defaultStrategy(source);

        noSkipStrategy(source);

        skipToNextStrategy(source);

        skipPastLastEventStrategy(source);

        skipToFirstStrategy(source);

        skipToLastStrategy(source);

        env.execute();
    }

    /**
     * 打印输出方法
     */
    private static class PrintSelectFunction implements PatternSelectFunction<LoginEvent, String> {
        @Override
        public String select(Map<String, List<LoginEvent>> map) throws Exception {
            StringBuilder stringBuilder = new StringBuilder();
            for (LoginEvent loginEvent : map.get("a")) {
                stringBuilder.append(loginEvent.getEventType());
            }
            for (LoginEvent loginEvent : map.get("b")) {
                stringBuilder.append(loginEvent.getEventType());
            }
            return stringBuilder.toString();
        }
    }

    private static void defaultStrategy(SingleOutputStreamOperator<LoginEvent> source) {
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("a").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("a");
                    }
                })
                .oneOrMore()
                .followedBy("b").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("b");
                    }
                });
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(item -> item.userId), pattern);
        patternStream.select(new PrintSelectFunction()).print("default");
    }

    private static void noSkipStrategy(SingleOutputStreamOperator<LoginEvent> source) {
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("a", AfterMatchSkipStrategy.noSkip()).where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("a");
                    }
                })
                .oneOrMore()
                .followedBy("b").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("b");
                    }
                });
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(item -> item.userId), pattern);
        patternStream.select(new PrintSelectFunction()).print("noSkip");
    }

    private static void skipToNextStrategy(SingleOutputStreamOperator<LoginEvent> source) {
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("a", AfterMatchSkipStrategy.skipToNext()).where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("a");
                    }
                })
                .oneOrMore()
                .followedBy("b").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("b");
                    }
                });
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(item -> item.userId), pattern);
        patternStream.select(new PrintSelectFunction()).print("skipToNext");
    }

    private static void skipPastLastEventStrategy(SingleOutputStreamOperator<LoginEvent> source) {
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("a", AfterMatchSkipStrategy.skipPastLastEvent()).where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("a");
                    }
                })
                .oneOrMore()
                .followedBy("b").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("b");
                    }
                });
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(item -> item.userId), pattern);
        patternStream.select(new PrintSelectFunction()).print("skipPastLastEvent");
    }

    private static void skipToFirstStrategy(SingleOutputStreamOperator<LoginEvent> source) {
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("a", AfterMatchSkipStrategy.skipToFirst("a")).where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("a");
                    }
                })
                .oneOrMore()
                .followedBy("b").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("b");
                    }
                });
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(item -> item.userId), pattern);
        patternStream.select(new PrintSelectFunction()).print("skipToFirst");
    }

    private static void skipToLastStrategy(SingleOutputStreamOperator<LoginEvent> source) {
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("a", AfterMatchSkipStrategy.skipToLast("a")).where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("a");
                    }
                })
                .oneOrMore()
                .followedBy("b").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.startsWith("b");
                    }
                });
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(item -> item.userId), pattern);
        patternStream.select(new PrintSelectFunction()).print("skipToLast");
    }
}

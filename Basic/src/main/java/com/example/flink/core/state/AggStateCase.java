package com.example.flink.core.state;

import com.example.flink.core.common.ClickSource;
import com.example.flink.core.common.beans.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 对用户点击事件流每 5 个数据统计一次平均时间戳。这是一个类似计数窗口（CountWindow）求平均值的计算
 */
public class AggStateCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                );

        stream.print("input");

        //自定义实现平均时间戳的统计
        stream.keyBy(Event::getUser)
                .flatMap(new AvgTsResult(5L))
                .print();

        env.execute();
    }

    //自定义实现RichFlatMapFunction
    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        //自定义的窗口
        private Long count;

        public AvgTsResult(Long count) {
            this.count = count;
        }

        //定义一个聚合状态，用来保存平均时间戳
        private AggregatingState<Event, Long> avgTsAggState;

        //定义一个值状态，保存用户访问的次数
        private ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取运行上下文
            //ACC : Tuple2<Long,Long> : 第一个所有时间戳的和，第二个是当前的个数
            //Out : 平均时间戳
            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //每来一条数据 count 就 +1
            Long currCount = countState.value();

            //判断，如果状态里面没有值的话，就+1，如果有值的话，就++
            if (currCount == null) {
                currCount = 1L;
            } else {
                currCount++;
            }

            //更新状态
            countState.update(currCount);
            avgTsAggState.add(value);

            //根据count,判断是否达到了累积的技术窗口的长度
            if (currCount.equals(count)) {
                out.collect(value.getUser() + "过去" + count + "次访问平均时间戳为：" + new Timestamp(avgTsAggState.get()));

                //清空状态
                avgTsAggState.clear();
                countState.clear();
            }
        }
    }
}



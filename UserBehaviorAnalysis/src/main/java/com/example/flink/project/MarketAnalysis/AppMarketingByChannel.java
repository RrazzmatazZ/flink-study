package com.example.flink.project.MarketAnalysis;

import com.example.flink.project.beans.ChannelPromotionCount;
import com.example.flink.project.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 市场营销分析
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<MarketingUserBehavior> source = env
                .addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        // 2. 分渠道开窗统计
        DataStream<ChannelPromotionCount> resultStream = source
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>(value.getChannel(), value.getBehavior());
                    }
                })
                // 定义滑窗
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print();

        env.execute();
    }

    // 实现自定义的增量聚合函数
    public static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现自定义的全窗口函数
    public static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> stringStringTuple2, ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String, String>, TimeWindow>.Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
            String channel = stringStringTuple2.f0;
            String behavior = stringStringTuple2.f1;
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = elements.iterator().next();
            out.collect(new ChannelPromotionCount(channel, behavior, windowEnd, count));
        }
    }

    // 实现自定义的模拟市场用户行为数据源
    public static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {
        // 控制是否正常运行的标识位
        Boolean running = true;

        // 定义用户行为和渠道的范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("appstore", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (running) {
                // 随机生成所有字段
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();
                // 发出数据
                ctx.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

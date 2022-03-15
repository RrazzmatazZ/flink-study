package com.example.flink.project.NetworkFlowAnalysis;

import com.example.flink.project.HotItemsAnalyze.HotItems;
import com.example.flink.project.HotItemsAnalyze.beans.ItemViewCount;
import com.example.flink.project.HotItemsAnalyze.beans.UserBehavior;
import com.example.flink.project.NetworkFlowAnalysis.beans.ApacheLogEvent;
import com.example.flink.project.NetworkFlowAnalysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class HotPages {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取文件，转换成pojo类型
        DataStreamSource<String> ds = env.readTextFile(HotItems.class.getResource("/apache.log").getPath());
        SingleOutputStreamOperator<ApacheLogEvent> sourceStream = ds
                .map(new MapFunction<String, ApacheLogEvent>() {
                    @Override
                    public ApacheLogEvent map(String value) throws Exception {
                        String[] fields = value.split(" ");
                        LocalDateTime dateTime = LocalDateTime.parse(fields[3], DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss"));
                        long timestamp = dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                        return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                    }
                })
                //时间是乱序的，且不是递增的
                //经过查验数据，乱序间隔大约在1分钟
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });

        //分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = sourceStream
                .filter((FilterFunction<ApacheLogEvent>) value -> "GET".equals(value.getMethod()))
                .filter( data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new PageCountAgg(), new PageCountResult());
        //收集同意窗口的count数据，排序后输出


        windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));
    }

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            long end = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new PageViewCount(s, end, count));
        }
    }

    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private final int n;

        public TopNHotPages(int n) {
            this.n = n;
        }

        ListState<PageViewCount> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("view-count-state", PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount value, KeyedProcessFunction<Long, PageViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，存入list中，并注册定时器，这里不用担心注册定时器过多，因为定时器是按照时间戳注册的。而每一批的windowEnd应该是相同的
            state.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(state.get().iterator());
            pageViewCounts.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

            StringBuilder sb = new StringBuilder();
            sb.append("=========================").append("\n");
            sb.append("窗口结束时间：").append(timestamp - 1).append("\n");
            for (int i = 0; i < Math.min(n, pageViewCounts.size()); i++) {
                sb.append("No").append(i + 1).append(":").append(pageViewCounts.get(i)).append("\n");
            }
            sb.append("=========================").append("\n");
            out.collect(sb.toString());
            Thread.sleep(1000L);
        }
    }
}

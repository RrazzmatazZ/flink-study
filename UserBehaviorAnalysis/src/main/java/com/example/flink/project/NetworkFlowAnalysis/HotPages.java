package com.example.flink.project.NetworkFlowAnalysis;

import com.example.flink.project.HotItemsAnalyze.HotItems;
import com.example.flink.project.beans.ApacheLogEvent;
import com.example.flink.project.beans.PageViewCount;
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
import org.apache.flink.util.OutputTag;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * 输入数据:
 * 83.149.9.216 - - 17/05/2015:10:25:49 +0000 GET /presentations/   --->因为窗口是5s（步长）的整数倍，所以实际从10:25:45s开始
 * 83.149.9.216 - - 17/05/2015:10:25:50 +0000 GET /presentations/
 * 83.149.9.216 - - 17/05/2015:10:25:51 +0000 GET /presentations/   ---->这里关闭第一个窗口，右为开区间，但这里还未输出topN（因为设置了Timer加了1s）
 * 83.149.9.216 - - 17/05/2015:10:25:52 +0000 GET /presentations/   ---->这里才开始输出topN
 * 83.149.9.216 - - 17/05/2015:10:25:55 +0000 GET /presentations/
 * 83.149.9.216 - - 17/05/2015:10:25:56 +0000 GET /presentations/
 * 83.149.9.216 - - 17/05/2015:10:25:56 +0000 GET /present
 * 83.149.9.216 - - 17/05/2015:10:25:57 +0000 GET /present
 * 83.149.9.216 - - 17/05/2015:10:26:01 +0000 GET /
 * 83.149.9.216 - - 17/05/2015:10:26:02 +0000 GET /pre
 * 83.149.9.216 - - 17/05/2015:10:25:46 +0000 GET /presentations/  ----> 这里开始测试延迟策略
 * 83.149.9.216 - - 17/05/2015:10:26:02 +0000 GET /pre
 * 83.149.9.216 - - 17/05/2015:10:26:03 +0000 GET /pre
 */
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
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });


        OutputTag<ApacheLogEvent> late = new OutputTag<ApacheLogEvent>("late") {
        };

        //分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = sourceStream
                .filter((FilterFunction<ApacheLogEvent>) value -> "GET".equals(value.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
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

            // state.clear();这里不能clear，因为  state.add(value); 只是更新当前的输入值，但是state里包含的不止是这一个输入值的key对于的状态
            //但是如果不clear的话，当有延迟数据进来的时候会出现同一个key输出多次的情况（因为注册了一个已经过时了的定时器，又由于是list类型的state，就相当于加了两次，所以要转为map state）,具体实现HotPagesUpdate
        }
    }
}

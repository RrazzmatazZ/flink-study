package com.example.flink.project.HotItemsAnalyze;

import com.example.flink.project.beans.ItemViewCount;
import com.example.flink.project.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 实时热门项目统计
 */
public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据，创建DataStream
        DataStreamSource<String> ds = env.readTextFile(HotItems.class.getResource("/UserBehavior.csv").getPath());

        //转换为pojo，分配时间戳和watermark
        DataStream<UserBehavior> map = ds.map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new UserBehavior(
                                new Long(split[0]),
                                new Long(split[1]),
                                new Integer(split[2]),
                                split[3],
                                new Long(split[4])
                        );
                    }
                })
                //因为这里数据是单调递增的，所以注册的为AscendingTimestampExtractor
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        //分组开窗聚合，得到每个窗口内各个商品的count值
        //1.先过滤pv行为（即点击行为，减少数据量级）
        //2.按照ItemId分组
        //3.开窗，窗口长度为1小时，步长为5分钟
        //4.窗口聚合，窗口输出
        SingleOutputStreamOperator<ItemViewCount> aggregate = map
                .filter((FilterFunction<UserBehavior>) value -> "pv".equals(value.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowResult());


        //收集同一窗口的所有商品count，输出topN
        SingleOutputStreamOperator<String> topN = aggregate
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(5));

        topN.print();

        env.execute("hot items");
    }

    /**
     * 实现窗口内的增量聚合函数
     */
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    /**
     * 实现全窗口函数
     */
    public static class WindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long end = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, end, count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        private final int n;

        public TopNHotItems(int n) {
            this.n = n;
        }

        ListState<ItemViewCount> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("view-count-state", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，存入list中，并注册定时器，这里不用担心注册定时器过多，因为定时器是按照时间戳注册的。而每一批的windowEnd应该是相同的
            state.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(state.get().iterator());
            itemViewCounts.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

            StringBuilder sb = new StringBuilder();
            sb.append("=========================").append("\n");
            sb.append("窗口结束时间：").append(timestamp - 1).append("\n");
            for (int i = 0; i < Math.min(n, itemViewCounts.size()); i++) {
                sb.append("No").append(i + 1).append(":").append(itemViewCounts.get(i)).append("\n");
            }
            sb.append("=========================").append("\n");
            out.collect(sb.toString());
            Thread.sleep(1000L);
        }
    }
}

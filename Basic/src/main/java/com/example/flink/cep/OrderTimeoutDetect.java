package com.example.flink.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 在电商平台中，最终创造收入和利润的是用户下单购买的环节。用户下单的行为可以表明用户对商品的需求，但在现实中，
 * 并不是每次下单都会被用户立刻支付。当拖延一段时间后，用户支付的意愿会降低。所以为了让用户更有紧迫感从而提高支付转化率，
 * 同时也为了防范订单支付环节的安全风险，电商网站往往会对订单状态进行监控，设置一个失效时间（比如 15分钟），
 * 如果下单后一段时间仍未支付，订单就会被取消。
 */
public class OrderTimeoutDetect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.获取订单事件流，并提取时间戳、生成水位线
        SingleOutputStreamOperator<OrderEvent> orderStream = env.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })

        );

        //2.定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() { //首先是下单事件
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() { //其次是支付事件,中间可以修改订单,宽松近邻
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                }).within(Time.minutes(15));// 要求在十五分钟之内完成

        //3.将应用模式应用到订单流上,检测匹配的复杂事件
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderStream.keyBy(event -> event.orderId), pattern);

        //4.定义一个侧输出流标签
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        //5.将完全匹配和超时部分匹配的复杂事件提取出来，进行处理
        SingleOutputStreamOperator<String> result = patternStream.process(new OrderPayMatch());

        // 将正常匹配和超时部分匹配的处理结果流打印输出
        result.print("正常支付");
        result.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    //实现自定义的 PatternProcessFunction，需实现 TimedOutPartialMatchHandler 接口
    public static class OrderPayMatch extends PatternProcessFunction<OrderEvent,String> implements TimedOutPartialMatchHandler<OrderEvent> {

        // 处理正常匹配事件
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context context, Collector<String> out) throws Exception {
            //获取当前的支付事件
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("用户" + payEvent.userId + "的订单：" + payEvent.orderId + " 已支付！");
        }

        //处理超时未支付事件
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context context) throws Exception {
            OrderEvent createEvent = match.get("create").get(0);
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
            context.output(timeoutTag,"用户" + createEvent.userId + "的订单 ：" + createEvent.orderId + " 超时未支付！");
        }
    }
}


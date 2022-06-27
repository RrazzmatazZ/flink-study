package com.example.flink.project.LoginFailDetect;

import com.example.flink.project.beans.LoginEvent;
import com.example.flink.project.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * 恶意登录拦截
 * <p>
 * 乱序数据
 */
public class LoginFailDetect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env
                .readTextFile(LoginFailDetect.class.getResource("/LoginLog.csv").getPath())
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.of(3, TimeUnit.SECONDS)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<LoginFailWarning> process = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarningNew(2));

        process.print();
        env.execute();
    }

    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        private final int maxFailTimes;

        public LoginFailDetectWarning(int i) {
            this.maxFailTimes = i;
        }

        // 定义状态：保存2秒内所有的登录失败事件
        ListState<LoginEvent> loginFailEventListState;
        // 定义状态：保存注册的定时器时间戳
        ValueState<Long> timerTsState;


        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断当前登录事件类型
            if ("fail".equals(value.getLoginState())) {
                // 1. 如果是失败事件，添加到表状态中
                loginFailEventListState.add(value);
                // 如果没有定时器，注册一个2秒之后的定时器
                if (null == timerTsState.value()) {
                    long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                } else {
                    // 2. 如果是登录成功，删除定时器，清空状态，重新开始
                    if (null != timerTsState.value()) {
                        ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                    }
                    loginFailEventListState.clear();
                    timerTsState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // 定时器触发，说明2秒内没有登录成功，判读ListState中失败的个数
            ArrayList<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get().iterator());
            int failTimes = loginFailEvents.size();
            if (failTimes >= maxFailTimes) {
                // 如果超出设定的最大失败次数，输出报警(侧输出)
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginFailEvents.get(0).getTimestamp(),
                        loginFailEvents.get(failTimes - 1).getTimestamp(),
                        "login fail in 2s for " + failTimes + " times"));
            }
            // 清空状态
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }

    /**
     * 代码实效性改进
     * 因为前面那种可能在2s内触发了很多次失败登录，但是因为触发器的原因在2s才触发
     * 还会有2s内前面很多条是失败而又有一条成功了，这种情况则不会触发报警
     */
    public static class LoginFailDetectWarningNew extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        // 定义属性，最大连续登录失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarningNew(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态：保存2秒内所有的登录失败事件
        ListState<LoginEvent> loginFailEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        }

        // 以登录事件作为判断报警的触发条件，不再注册定时器
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断当前事件登录状态
            if ("fail".equals(value.getLoginState())) {
                // 1. 如果是登录失败，获取状态中之前的登录失败事件，继续判断是否已有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 1.1 如果已经有登录失败事件，继续判断时间戳是否在2秒之内
                    // 获取已有的登录失败事件
                    LoginEvent firstFailEvent = iterator.next();

                    //TODO 这里需要考虑乱序问题,乱序的时候不能输出（参考LoginLog1.csv）
                    if (value.getTimestamp() - firstFailEvent.getTimestamp() <= 2) {
                        // 1.1.1 如果在2秒之内，输出报警
                        out.collect(new LoginFailWarning(value.getUserId(), firstFailEvent.getTimestamp(), value.getTimestamp(), "login fail 2 times in 2s"));
                    }

                    // 不管报不报警，这次都已处理完毕，直接更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(value);
                } else {
                    // 1.2 如果没有登录失败，直接将当前事件存入ListState
                    loginFailEventListState.add(value);
                }
            } else {
                // 2. 如果是登录成功，直接清空状态
                loginFailEventListState.clear();
            }
        }
    }
}

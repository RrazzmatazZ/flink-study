package com.example.flink.core.common;

import com.example.flink.core.common.beans.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    //声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"Mary","Alice","Bob","Cary"};
        String[] urls = {"./home","./cart","./prod?id=100","./prod?id=10","./fav"};


        //循环生成数据
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            //获取当前系统的时间
            long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timestamp));
            //生产频率慢一点
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
package com.example.flink.project.HotItemsAnalyze;

import com.example.flink.project.beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 实时热门项目统计SQL实现
 */
public class HotItemsWithSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据，创建DataStream
        DataStreamSource<String> ds = env.readTextFile(HotItems.class.getResource("/UserBehavior.csv").getPath());
        //转换为pojo，分配时间戳和watermark
        DataStream<UserBehavior> map = ds.map((MapFunction<String, UserBehavior>) value -> {
                    String[] split = value.split(",");
                    return new UserBehavior(
                            new Long(split[0]),
                            new Long(split[1]),
                            new Integer(split[2]),
                            split[3],
                            new Long(split[4])
                    );
                })
                //因为这里数据是单调递增的，所以注册的为AscendingTimestampExtractor
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

        //将流转换为表
        Table table = tableEnv.fromDataStream(map, "itemId,behavior,timestamp.rowtime as ts");


        //分组开窗
        Table windowAggTable = table
                .filter("behavior ='pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as cnt");

        //利用开窗函数对count值进行排序，并获取rowNum,从而获取topN
        //这里因为调用的是tableEnvironment而不是StreamTableEnvironment（可能获取field会出现问题）
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId,windowEnd,cnt");

        Table result = tableEnv.sqlQuery(
                "select * from (" +
                        "select *,ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from agg" +
                        ") where row_num<=5"
        );

        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(result, Row.class);
        resultStream.print();
        env.execute("hot items sql");

    }
}

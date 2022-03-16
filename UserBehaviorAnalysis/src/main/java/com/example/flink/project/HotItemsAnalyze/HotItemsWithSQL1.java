package com.example.flink.project.HotItemsAnalyze;

import com.example.flink.project.beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 实时热门项目统计SQL实现
 */
public class HotItemsWithSQL1 {

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

        tableEnv.createTemporaryView("dataTable", map, "itemId,behavior,timestamp.rowtime as ts");

        Table resultTable = tableEnv.sqlQuery(
                "     select * from (" +
                        "select *,ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from (" +
                        "  select itemId,count(itemId) as cnt,HOP_END(ts,interval '5' minute,interval '1' hour) as windowEnd" +
                        "  from dataTable" +
                        "  where behavior='pv'" +
                        "  group by itemId,HOP(ts,interval '5' minute,interval '1' hour)" +
                        ")" +
                        ") where row_num<=5"
        );

        DataStream<Tuple2<Boolean, Row>> outStream = tableEnv.toRetractStream(resultTable, Row.class);
        outStream.print();
        env.execute();

    }
}

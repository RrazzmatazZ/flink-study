package com.example.flink.core.source;

import com.example.flink.wordcount.WordCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * socket source
 */
public class SourceFromSocket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        int port = tool.getInt("port");
        DataStreamSource<String> ds = env.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> sum = ds.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        sum.print();
        env.execute();
    }
}

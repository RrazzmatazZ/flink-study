package com.example.flink.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.readTextFile(WordCountStream.class.getResource("/wordcount.txt").getPath());
        DataStream<Tuple2<String, Integer>> sum = ds.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        sum.print();
        env.execute();
    }
}

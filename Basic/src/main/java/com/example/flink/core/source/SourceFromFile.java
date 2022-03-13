package com.example.flink.core.source;

import com.example.flink.wordcount.WordCountStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceFromFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.readTextFile(WordCountStream.class.getResource("/wordcount.txt").getPath());
        ds.print();
        env.execute();
    }
}

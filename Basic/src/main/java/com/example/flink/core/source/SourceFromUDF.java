package com.example.flink.core.source;

import com.example.flink.core.common.beans.SensorReading;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义source
 */
public class SourceFromUDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> udfSource = env.addSource(new MySensor());
        udfSource.print();
        env.execute();
    }

    public static class MySensor implements SourceFunction<SensorReading> {

        private volatile boolean isRunning = true;

        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                String sensorName = "sensor_" + random.nextInt();
                Double temperature = 60 + random.nextGaussian() * 20;
                ctx.collect(new SensorReading(sensorName, System.currentTimeMillis(), temperature));
                Thread.sleep(1000L);
            }
        }

        public void cancel() {
            isRunning = false;
        }
    }

    public static class MyParallelSensor implements ParallelSourceFunction<SensorReading> {

        private volatile boolean isRunning = true;

        final Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (isRunning) {
                String sensorName = "sensor_" + random.nextInt();
                Double temperature = 60 + random.nextGaussian() * 20;
                ctx.collect(new SensorReading(sensorName, System.currentTimeMillis(), temperature));

            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}

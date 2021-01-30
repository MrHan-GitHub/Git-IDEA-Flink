package com.hgj.day05;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_test {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPunctuated(2000L);
            }
        }
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor> vc = waterSensorStringKeyedStream.sum("vc");

        vc.print();

        env.execute();

    }

    public static class MyPunctuated implements WatermarkGenerator<WaterSensor> {
        // 允许的最大延迟时间 ms
        private final long maxDelay;
        private long maxTs;

        public MyPunctuated(long maxDelay) {
            this.maxDelay = maxDelay * 1000;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }


        // 每收到一个元素, 执行一次. 用来生产WaterMark中的时间戳
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("onEvent..." + eventTimestamp);
            //有了新的元素找到最大的时间戳
            maxTs = Math.max(maxTs, eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1));
        }


        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要实现
        }
    }


}

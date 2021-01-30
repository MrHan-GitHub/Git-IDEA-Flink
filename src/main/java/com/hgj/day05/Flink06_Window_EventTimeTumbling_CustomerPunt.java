package com.hgj.day05;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Scanner;

/**
 *  滚动窗口  (自定义WaterMark  间歇性  一条数据生成一次WaterMark)
 *
 */
public class Flink06_Window_EventTimeTumbling_CustomerPunt {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPunt(2000L);
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

        WindowedStream<WaterSensor, String, TimeWindow> window = waterSensorStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<WaterSensor> vc = window.sum("vc");

        vc.print();

        env.execute();

    }

    public static class MyPunt implements WatermarkGenerator<WaterSensor>{

        private Long maxTs;
        private Long maxDelay;

        public MyPunt(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + maxDelay + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("取数据中最大的时间戳");
            maxTs = Math.max(maxTs,eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - maxDelay));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}

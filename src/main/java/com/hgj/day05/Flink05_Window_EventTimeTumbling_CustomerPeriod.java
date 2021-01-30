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

/**
 *  滚动窗口  自定义WaterMark(周期型生成WaterMark)
 *
 */

public class Flink05_Window_EventTimeTumbling_CustomerPeriod {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriod(2000L);
            }
        }
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });


        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, Long> waterSensorLongKeyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getTs);

        WindowedStream<WaterSensor, Long, TimeWindow> window = waterSensorLongKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<WaterSensor> vc = window.sum("vc");

        vc.print();

        env.execute();

    }

    //自定义周期性的WaterMark生成器
    public static class MyPeriod implements WatermarkGenerator<WaterSensor>{

        //要保证WaterMark是自增的，所以要保留之前的Watermark和新进来的数据相比较
        private Long maxTs;

        private Long maxDelay;

        public MyPeriod(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + maxDelay + 1;
        }

        //当数据来的时候调用
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            //System.out.println("数据中最大的时间戳");
            maxTs = Math.max(maxTs,eventTimestamp);
        }

        //周期性的调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //把Watermark写出去
            //System.out.println("生成Watermark" + (maxTs - maxDelay));
            output.emitWatermark(new Watermark(maxTs - maxDelay));
        }
    }
}

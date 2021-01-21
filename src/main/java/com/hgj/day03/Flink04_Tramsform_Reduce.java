package com.hgj.day03;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Tramsform_Reduce {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        DataStreamSink<WaterSensor> dataStreamSink = source.map(new Flink03_Tramsform_MaxBy.myMapFunction())
                .keyBy(new Flink03_Tramsform_MaxBy.myKeySelector())
                .reduce(new myReduceFunction())
                .print();

        env.execute();

    }

    public static class myReduceFunction implements ReduceFunction<WaterSensor>{

        @Override
        public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
            return new WaterSensor(value1.getId(),value2.getTs(),Math.max(value1.getVc(),value2.getVc()));
        }
    }
}

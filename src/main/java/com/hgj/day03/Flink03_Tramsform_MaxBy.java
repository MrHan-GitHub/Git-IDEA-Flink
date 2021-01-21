package com.hgj.day03;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Tramsform_MaxBy {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new myMapFunction());

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = map.keyBy(new myKeySelector());

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorStringKeyedStream.maxBy("vc",false);

        waterSensorSingleOutputStreamOperator.print();

        env.execute();

    }

    public static class myMapFunction implements MapFunction<String,WaterSensor>{

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        }
    }

    public static class myKeySelector implements KeySelector<WaterSensor,String>{

        @Override
        public String getKey(WaterSensor value) throws Exception {

            return value.getId();
        }
    }
}

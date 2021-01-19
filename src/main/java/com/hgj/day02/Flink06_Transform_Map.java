package com.hgj.day02;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_Map {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/WaterSensor.txt");

        SingleOutputStreamOperator<WaterSensor> map = source.map(new myMapFunction());
        //SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
        //    @Override
        //    public WaterSensor map(String value) throws Exception {
        //        String[] split = value.split(",");
        //        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        //    }
        //});

        map.print();

        env.execute();

    }

    public static class myMapFunction implements MapFunction<String,WaterSensor>{

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }
    }
}

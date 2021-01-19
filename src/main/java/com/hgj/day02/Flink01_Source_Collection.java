package com.hgj.day02;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建集合
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));

        //3.读取数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromCollection(waterSensors);

        //4.打印数据
        waterSensorDataStreamSource.print();

        //5.执行
        env.execute();


    }
}

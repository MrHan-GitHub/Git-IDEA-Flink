package com.hgj.day02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * open方法的调用次数跟并行度有关，这个Task会分成两个SubTask，
 * 之后每个SubTask都会进入到一个Slot中，没有个Slot都会调用一次open方法。
 *
 */

public class Flink08_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> source = env.readTextFile("input/WaterSensor.txt");

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = source.flatMap(new myRichFlatMapFunction());

        stringSingleOutputStreamOperator.print();

        env.execute();


    }

    public static class myRichFlatMapFunction extends RichFlatMapFunction<String,String>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open方法被调用");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] split = value.split(",");
            for (String s : split) {
                out.collect(s);
            }
        }
    }
}

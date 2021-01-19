package com.hgj.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * open方法的调用次数跟并行度有关，这个Task会分成两个SubTask，
 * 之后每个SubTask都会进入到一个Slot中，没有个Slot都会调用一次open方法。
 *
 */

public class Flink09_Transform_RichFilter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> source = env.readTextFile("input/WaterSensor.txt");

        SingleOutputStreamOperator<String> filter = source.filter(new myRichFilterFunction());

        filter.print();

        env.execute();

    }

    public static class myRichFilterFunction extends RichFilterFunction<String>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open方法被调用");
        }

        @Override
        public boolean filter(String value) throws Exception {
            String[] split = value.split(",");
            return Integer.parseInt(split[2]) > 30;
        }
    }
}

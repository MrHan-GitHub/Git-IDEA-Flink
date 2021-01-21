package com.hgj.practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class day03_Test02_Distinct {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        HashSet<String> strings = new HashSet<>();

        SingleOutputStreamOperator<String> setSingleOutputStreamOperator = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        setSingleOutputStreamOperator.keyBy(x -> x).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (strings.contains(value)){
                    return false;
                }else {
                    strings.add(value);
                    return true;
                }
            }
        }).print();

        env.execute();

    }
}

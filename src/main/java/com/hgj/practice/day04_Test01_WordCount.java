package com.hgj.practice;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class day04_Test01_WordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //DataStreamSource<String> source = env.readTextFile("input/Word.txt");

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(new Tuple2<String, Integer>(s1, 1));
                }
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> process = map.keyBy(value -> value.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private HashMap<String,Integer> hashMap = new HashMap<>();


            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                Integer count = hashMap.getOrDefault(value.f0, 0);
                count++;
                hashMap.put(value.f0,count);
                out.collect(new Tuple2<>(value.f0, count));
            }
        });

        process.print();

        env.execute();


    }
}

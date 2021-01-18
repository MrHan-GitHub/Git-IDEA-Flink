package com.hgj.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("F:\\IDEA\\code\\Flink\\input\\wordcount\\Word.txt");

        SingleOutputStreamOperator<String> wordDS = source.flatMap(new Flink01_WordCount_batch.MyFlatMapFunction());

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //KeyedStream<Tuple2<String, Integer>, Object> wordToOneKS = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
        //    @Override
        //    public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        //        return stringIntegerTuple2.f0;
        //    }
        //});

        KeyedStream<Tuple2<String, Integer>, Tuple> wordToOneKS = wordToOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordToOneKS.sum(1);

        result.print();

        env.execute();


    }
}

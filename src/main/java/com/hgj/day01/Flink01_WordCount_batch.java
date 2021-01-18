package com.hgj.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_batch {
    public static void main(String[] args) throws Exception {

        //1.创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件
        DataSource<String> input = env.readTextFile("F:\\IDEA\\code\\Flink\\input\\wordcount\\Word.txt");

        //3.
        FlatMapOperator<String, String> WordDS = input.flatMap(new MyFlatMapFunction());

        //4.
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS = WordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //5.
        UnsortedGrouping<Tuple2<String, Integer>> groupDS = wordToOneDS.groupBy(0);

        //6.
        AggregateOperator<Tuple2<String, Integer>> resultDS = groupDS.sum(1);

        //7.
        resultDS.print();



    }

    public static class MyFlatMapFunction implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(s2);
            }
        }
    }

}

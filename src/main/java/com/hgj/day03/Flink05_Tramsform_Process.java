package com.hgj.day03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_Tramsform_Process {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> process = source.process(new myProcessFunction());

        process.keyBy(x->x.f0).sum(1).print();

        env.execute();

    }

    public static class myProcessFunction extends ProcessFunction<String,Tuple2<String,Integer>>{

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
            String[] s = value.split(" ");
            for (String s1 : s) {
                out.collect(new Tuple2<>(s1,1));
            }
        }
    }
}

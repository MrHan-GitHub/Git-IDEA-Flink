package com.hgj.day04;

import com.hgj.been.UserBehavior;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_PVCount_Process {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> map = source.map(new Flink01_PVCount_WordCount.myUserBehavior());

        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new Flink01_PVCount_WordCount.myfliterFunction());

        SingleOutputStreamOperator<Tuple2<String, Integer>> map1 = filter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>(value.getBehavior(), 1);
            }
        });

        map1.keyBy(value -> value.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Long>() {
            Long count = 0L;
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Long> out) throws Exception {
                count ++;
                out.collect(count);
            }
        }).print();

        env.execute();

    }
}

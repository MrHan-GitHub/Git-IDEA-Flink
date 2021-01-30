package com.hgj.day04;

import com.hgj.been.UserBehavior;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink03_UVCount {
    private static HashSet<Long> uids = new HashSet<>();


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> source = env.readTextFile("hdfs://hadoop102:8020/flink/UserBehavior.csv");

        //DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");


        SingleOutputStreamOperator<UserBehavior> map = source.map(new Flink01_PVCount_WordCount.myUserBehavior());

        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new Flink01_PVCount_WordCount.myfliterFunction());

        //KeyedStream<UserBehavior, String> keyBy = filter.keyBy(value -> "uv");

        KeyedStream<UserBehavior,
                Long> keyBy = filter.keyBy(value -> value.getUserId());


        keyBy.process(new KeyedProcessFunction<Long, UserBehavior, Integer>() {


            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                    uids.add(value.getUserId());
                    out.collect(uids.size());
            }
        }).print();


        env.execute();


    }
}

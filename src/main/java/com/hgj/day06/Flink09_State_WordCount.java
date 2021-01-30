package com.hgj.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME","atguigu");

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        //开启ck
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //cancel任务时不删除ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //读取端口数据并转化成元组
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(new Tuple2<>(s1,1));
                }
            }
        }).keyBy(value -> value.f0).sum(1).print();

        env.execute();
    }
}

package com.hgj.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class Flink03_WordCount_UnBounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(2);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> wordDS = source.flatMap(new Flink01_WordCount_batch.MyFlatMapFunction());

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> windowAll = wordToOneDS.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //KeyedStream<Tuple2<String, Integer>, String> WordToOneKS = wordToOneDS.keyBy(value -> value.f0);

        windowAll.sum(1).print();

        env.execute();
    }
}

package com.hgj.day04;

import com.hgj.Utils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 滚动窗口(按条数 )  countWindow
 */

public class Flink12_Window_CountSlide {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = source.flatMap(new Utils.myFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyByDS = wordToOneDS.keyBy(value -> value.f0);

        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow = keyByDS.countWindow(5L, 2L);

        countWindow.sum(1).print();

        env.execute();
    }
}

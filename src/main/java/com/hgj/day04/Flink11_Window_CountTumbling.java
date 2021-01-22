package com.hgj.day04;

import com.hgj.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 *  滚动窗口（按条数） countWindow
 *
 */
public class Flink11_Window_CountTumbling {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> WordToOneDS = source.flatMap(new Utils.myFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyBy = WordToOneDS.keyBy(value -> value.f0);

        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow = keyBy.countWindow(5L);

        countWindow.sum(1).print();

        env.execute();
    }
}

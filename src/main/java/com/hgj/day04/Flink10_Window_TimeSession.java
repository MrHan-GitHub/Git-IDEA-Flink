package com.hgj.day04;

import com.hgj.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *  会话窗口
 *
 */

public class Flink10_Window_TimeSession {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDs = source.flatMap(new Utils.myFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyByDs = wordToOneDs.keyBy(value -> value.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyByDs.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        window.sum(1).print();

        env.execute();


    }
}

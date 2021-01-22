package com.hgj.day04;

import com.hgj.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *  滑动窗口
 *
 */
public class Flink09_Window_TimeSlide {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = source.flatMap(new Utils.myFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToOneDS.keyBy(value -> value.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyBy.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);

        sum.print();

        env.execute();

    }
}

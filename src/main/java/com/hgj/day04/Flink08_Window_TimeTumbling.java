package com.hgj.day04;

import com.google.common.collect.Lists;
import com.hgj.Utils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 *  滚动窗口  5S  一个窗口
 *
 */

public class Flink08_Window_TimeTumbling {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = source.flatMap(new Utils.myFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyByDS = wordToOneDS.keyBy(value -> value.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyByDS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //window.sum(1).print();
        //增量
        //SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = window.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
        //    @Override
        //    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        //        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        //    }
        //});

        //在aggregate中使用两个参数，第一个参数是增量，第二个是全量，这是因为在只有增量的情况下返回值结果没有办法获取key，
        // 所以在后面增加了一个全量来获取key，之后将结果封装写出。
        // 在增量中来一条数据就计算一次，在后面的全量处理中就已经得到了这个word出现的次数，
        // 所以在其收集数据是就只收集到了一条数据(迭代器中只有一条数据)，因此直接拿迭代器中第一条数据即可，再封装上key就可以写出了。
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggregate = window.aggregate(new MyAggFunc(), new myWindowFunc());


        //全量
        //SingleOutputStreamOperator<Tuple2<String, Integer>> apply = window.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
        //    @Override
        //    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
        //        //取出迭代器长度
        //        int size = Lists.newArrayList(input).size();
        //        //输出数据
        //        out.collect(new Tuple2<>(key, size));
        //    }
        //});

        //SingleOutputStreamOperator<Tuple2<String, Integer>> process = window.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
        //    @Override
        //    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        //        //取出迭代器的长度
        //        int size = Lists.newArrayList(elements).size();
        //        //输出数据
        //        out.collect(new Tuple2<>(new Timestamp(context.window().getStart()) + ":" + key, size));
        //    }
        //});

        aggregate.print();

        env.execute();


    }

    public static class MyAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class myWindowFunc implements WindowFunction<Integer,Tuple2<String,Integer>,String,TimeWindow>{
        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer next = input.iterator().next();
            out.collect(new Tuple2<String,Integer>(key,next));
        }
    }

}

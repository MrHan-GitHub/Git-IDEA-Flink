package com.hgj.day04;

import com.hgj.been.AdsClickLog;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 各省份页面广告点击量实时统计
 */
public class Flink06_Project_Ads_Click {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");

        //转化成AdsClickLog对象
        SingleOutputStreamOperator<AdsClickLog> map = source.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //封装成((省份，广告id),1)
        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Long>> map1 = map.map(new MapFunction<AdsClickLog, Tuple2<Tuple2<String, Long>, Long>>() {
            @Override
            public Tuple2<Tuple2<String, Long>, Long> map(AdsClickLog value) throws Exception {

                return new Tuple2<Tuple2<String, Long>, Long>(new Tuple2<String, Long>(value.getProvince(), value.getAdId()), 1L);
            }
        });

        //按照(省份，广告id)进行分组
        KeyedStream<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>> tuple2Tuple2KeyedStream = map1.keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                return new Tuple2<>(value.f0.f0, value.f0.f1);
            }
        });

        tuple2Tuple2KeyedStream.sum(1).print();

        env.execute();

    }

}

package com.hgj.day04;

import com.hgj.been.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 不分渠道
 */

public class Flink05_Project_AppAnalysis {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDataStreamSource = env.addSource(new Flink04_Project_AppAnalysis_By_Chanel.AppMarketingDataSource());

        SingleOutputStreamOperator<Tuple2<String, Long>> map = marketingUserBehaviorDataStreamSource.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                return new Tuple2<String, Long>(value.getBehavior(), 1L);
            }
        });

        map.keyBy(value -> value.f0).sum(1).print();

        env.execute();

    }
}

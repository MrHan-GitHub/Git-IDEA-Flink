package com.hgj.day07;

import com.hgj.been.UserBehavior;
import com.hgj.been.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * 指定时间范围内网站独立访客数（UV）的统计
 *
 */

public class Flink04_Project_Product_UV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = source.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(
                    Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4])
            );
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        SingleOutputStreamOperator<UserVisitorCount> result = userBehaviorDS.keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new UserVisitorProcessWindowFunc());

        result.print();

        env.execute();
    }

    public static class UserVisitorProcessWindowFunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount,String, TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
            //创建hashset用于去重
            HashSet<Long> uids = new HashSet<>();

            //取出所有元素
            Iterator<UserBehavior> iterator = elements.iterator();

            //遍历迭代器，将数据的uid放入set中去重
            while (iterator.hasNext()) {
                uids.add(iterator.next().getUserId());
            }

            //输出数据
            out.collect(new UserVisitorCount("UV",
                    new Timestamp(context.window().getEnd()).toString(),
                    uids.size()));
        }
    }
}

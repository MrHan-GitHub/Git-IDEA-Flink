package com.hgj.day12;

import com.hgj.been.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_ItemCountTopNWithSQL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取文本数据
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
            return new UserBehavior(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        //讲流转化成表
        Table table = tableEnv.fromDataStream(userBehaviorDS,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timestamp"),
                $("rt").rowtime());

        Table result = tableEnv.sqlQuery("select " +
                "* " +
                "from " +
                "(" +
                "select " +
                "*," +
                "ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY itemId_count DESC) as row_num " +
                "from" +
                "(" +
                "select " +
                "itemId," +
                "count(itemId) as itemId_count, " +
                "HOP_END(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd " +
                "FROM " +
                table +
                " GROUP BY itemId, HOP(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)" +
                ")" +
                ") " +
                "WHERE row_num <= 5");



        //创建临时表
        //tableEnv.createTemporaryView("userBehavior",userBehaviorDS,$("itemId"),$("timestamp").as("ts"));
        //
        ////基于事件时间，开滑动窗口
        //Table result = table.window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("rt")).as("tw"))
        //        .groupBy($("itemId"),$("tw"))
        //        .select($("itemId"), $("itemId").count().as("ct"))
        //        .orderBy($("timestamp"));
        //
        ////表转流
        //tableEnv.toRetractStream(result, Row.class).print();
        result.execute().print();

        //执行任务
        env.execute();

    }
}

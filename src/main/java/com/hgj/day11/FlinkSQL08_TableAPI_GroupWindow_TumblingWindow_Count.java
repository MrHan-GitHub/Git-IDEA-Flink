package com.hgj.day11;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkSQL08_TableAPI_GroupWindow_TumblingWindow_Count {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据并转化成JavaBean对象
        SingleOutputStreamOperator<WaterSensor> watersensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //流转化成表，并执行处理时间
        Table table = tableEnv.fromDataStream(watersensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pc").proctime());

        //开滚动窗口计算wordcount
        Table result = table.window(Tumble.over(rowInterval(5L)).on($("pc")).as("cw"))
                .groupBy($("id"), $("cw"))
                .select($("id"), $("id").count());

        //表转流输出
        tableEnv.toRetractStream(result, Row.class).print();

        //执行任务
        env.execute();

    }
}

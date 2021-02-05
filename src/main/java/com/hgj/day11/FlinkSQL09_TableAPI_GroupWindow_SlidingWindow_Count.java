package com.hgj.day11;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rowInterval;

public class FlinkSQL09_TableAPI_GroupWindow_SlidingWindow_Count {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读端口数据转化成JavaBean
        SingleOutputStreamOperator<WaterSensor> WaterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //转化成表
        Table table = tableEnv.fromDataStream(WaterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        //这里有bug，说是滑动窗口，但是只有当组内的数据到达5条才会计算
        //开窗
        Table result = table.window(Slide
                .over(rowInterval(5L))
                .every(rowInterval(2L))
                .on($("pt"))
                .as("cw"))
                .groupBy($("id"),$("cw"))
                .select($("id"), $("id").count());

        //结果转化成流输出
        tableEnv.toAppendStream(result, Row.class).print();

        //执行任务
        env.execute();

    }
}

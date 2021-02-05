package com.hgj.day11;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class FlinkSQL05_TableAPI_GroupWindow_TumblingWindow_ProcessTime {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据创建流并转换每一行数据为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDs = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //将流转化成表并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorDs,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        //开滚动窗口计算wordcount
        Table result = table.window(Tumble.over(lit(5).seconds()).on($("pt")).as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count());

        //将结果表转化为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();

        //执行任务
        env.execute();
    }
}

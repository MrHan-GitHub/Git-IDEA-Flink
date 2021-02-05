package com.hgj.day11;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class FlinkSQL07_TableAPI_GroupWindow_SessionWindow_PorcessTime {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据并转化成JavaBean对象
        SingleOutputStreamOperator<WaterSensor> WatersensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //将其转化成表
        Table table = tableEnv.fromDataStream(WatersensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        //开启会话窗口
        Table result = table.window(Session.withGap(lit(5).seconds()).on("pt").as("sw"))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());

        //窗口相当于增量窗口，来一条计算一条，在窗口关闭的时候输出，所以说这里采用追加和撤回流都可以，没有影响。
        //转化成流输出
        tableEnv.toRetractStream(result, Row.class).print();

        //执行任务
        env.execute();
    }
}

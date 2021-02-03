package com.hgj.day10;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_StreamToTable_Agg {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> WaterSensor = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(
                            split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2])
                    );
                });

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转化成动态表
        Table sensortable = tableEnv.fromDataStream(WaterSensor);

        //使用TableAPI 实现 select id,sum(vc) from sensor where vc>=20 group by id;
        Table selectTable = sensortable.where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));

        //将selectTable转化成流输出
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(selectTable, Row.class);

        //打印
        rowDataStream.print();

        //执行任务
        env.execute();
    }
}

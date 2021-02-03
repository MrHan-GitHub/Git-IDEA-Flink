package com.hgj.day10;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL09_SQL_Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据转化为javaBean
        SingleOutputStreamOperator<WaterSensor> WaterSensor = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        //将流转化成状态表
        Table sensorTable = tableEnv.fromDataStream(WaterSensor);

        //使用SQL查询未注册的表
        Table result = tableEnv.sqlQuery("select id,sum(vc) from " + sensorTable + " group by id");

        //将表对象转化成流进行过打印输出
        tableEnv.toRetractStream(result, Row.class).print();

        //执行任务
        env.execute();

    }
}

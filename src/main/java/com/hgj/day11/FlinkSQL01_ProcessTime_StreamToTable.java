package com.hgj.day11;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_ProcessTime_StreamToTable {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取文本数据创建流并转化成JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDs = env.readTextFile("input/WaterSensor.txt")
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(
                            split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2])
                    );
                });

        //将流传话成表，并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorDs,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        //打印元数据信息
        table.printSchema();

    }
}

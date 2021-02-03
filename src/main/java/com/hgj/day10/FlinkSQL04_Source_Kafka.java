package com.hgj.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL04_Source_Kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用连接器的方式读取Kafka数据
        tableEnv.connect(new Kafka()
        .version("universal")
        .topic("test")
        //.startFromEarliest()
        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
        .property(ConsumerConfig.GROUP_ID_CONFIG,"BigData0821_1"))
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("vc",DataTypes.INT()))
                //.withFormat(new Csv())
                .withFormat(new Json())
                .createTemporaryTable("sensor");

        //使用连接器创建表
        Table sensor = tableEnv.from("sensor");

        //查询数据
        Table selectTable = sensor.groupBy($("id"))
                .select($("id"), $("id").count());

        //将表转化为流输出
        tableEnv.toRetractStream(selectTable, Row.class).print();

        //执行任务
        env.execute();

    }
}

package com.hgj.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL03_Source_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用Connect方式读取文本数据
        tableEnv.connect(new FileSystem().path("input/WaterSensor.txt"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Csv())
                .createTemporaryTable("sensor");

        //将连接器应用，转化成表
        Table sensor = tableEnv.from("sensor");

        //查询
        Table resultTable = sensor.groupBy($("id"))
                .select($("id"), $("id").count().as("ct"));

        //转化为流进行输出
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultTable, Row.class);

        //打印数据
        tuple2DataStream.print();

        //执行任务
        env.execute();

    }
}

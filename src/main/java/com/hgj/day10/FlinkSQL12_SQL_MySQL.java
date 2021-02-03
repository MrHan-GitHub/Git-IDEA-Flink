package com.hgj.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL12_SQL_MySQL {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //注册SourceTable
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_source',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'properties.group.id' = 'atguigu',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'csv'"
                + ")");

        //注册SinkTable：MySQL,表并不会自动创建
        tableEnv.executeSql("create table sink_sensor(id string,ts bigint,vc int) with("
                + "'connector' = 'jdbc',"
                + "'url' = 'jdbc:mysql://hadoop102:3306/test',"
                + "'table-name' = 'sink_table',"
                + "'username' = 'root',"
                + "'password' = '123456'"
                + ")");

        ////执行查询Kafka数据
        //Table source_sensor = tableEnv.from("source_sensor");
        ////将数据写入MySQL
        //source_sensor.executeInsert("sink_sensor");

        //将数据写入MySQL
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor");

    }
}

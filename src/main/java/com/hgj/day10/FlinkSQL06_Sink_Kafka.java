package com.hgj.day10;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL06_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据并转化成javaBean
        SingleOutputStreamOperator<WaterSensor> WaterSensorDs = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //将流转化成动态表
        Table sensorTable = tableEnv.fromDataStream(WaterSensorDs);

        //使用TableAPI过滤出“ws_001”的数据
        Table selectTable = sensorTable
                .where($("id").isEqual("ws_001"))
                .select($("id"), $("ts"), $("vc"));

        //将selectTable写入Kafka
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test")
                .startFromLatest()
                .sinkPartitionerRoundRobin()
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                //注意，这里有一些小bug，Csv是打印的时候数据与之间会有空格，Json打印的时候就没有这个小Bug
                //.withFormat(new Csv())
                .withFormat(new Json())
                .createTemporaryTable("sensor");

        selectTable.executeInsert("sensor");

        //执行任务
        env.execute();

    }
}

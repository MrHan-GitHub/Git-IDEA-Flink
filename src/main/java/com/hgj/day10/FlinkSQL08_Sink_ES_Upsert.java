package com.hgj.day10;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL08_Sink_ES_Upsert {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //从端口读数据转化成JavaBean
        SingleOutputStreamOperator<WaterSensor> WaterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(
                            split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2])
                    );
                });

        //将流转化成状态表
        Table sensorTable = tableEnv.fromDataStream(WaterSensorDS);

        //使用TableAPI按照id和vc分组求id,count(),vc
        Table selectTable = sensorTable
                .groupBy($("id"),$("vc"))
                .select($("id"),
                        $("ts").count().as("ts_ct"),
                        $("vc"));

        //将selectTable写入ES
        tableEnv.connect(new Elasticsearch()
                .index("sensor_sql")
                .documentType("_doc")
                .version("6")
                .host("hadoop102", 9200, "http")
                .bulkFlushMaxActions(1))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Json())
                .inUpsertMode()
                .createTemporaryTable("sensor");

        selectTable.executeInsert("sensor");

        env.execute();

    }
}

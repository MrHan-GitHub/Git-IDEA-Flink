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

public class FlinkSQL01_StreamToTable_Test {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> WaterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //创建表执行环境
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);

        //将流转化成动态表
        Table sensorTable = tableenv.fromDataStream(WaterSensorDS);

        //使用TableAPI过滤出“ws_001”的数据
        //Table selectTable = sensorTable.where($("id").isEqual("ws_001"))
        //        .select($("id"), $("ts"), $("vc"));
        Table selectTable = sensorTable
                .where("id = 'ws_001'")
                .select("id,ts,vc");

        //将selectTable转化成流输出
        //DataStream<Row> rowDataStream = tableenv.toAppendStream(selectTable, Row.class);
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableenv.toRetractStream(selectTable, Row.class);

        //打印
        rowDataStream.print();

        //执行
        env.execute();
    }

}

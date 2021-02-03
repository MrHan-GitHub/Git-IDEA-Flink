package com.hgj.day10;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL05_Sink_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据并转化成JavaBean对象
        SingleOutputStreamOperator<WaterSensor> WaterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(
                            split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2])
                    );
                });

        //将流转化成动态表
        Table sensorTable = tableEnv.fromDataStream(WaterSensorDS);

        //使用tableAPI过滤出“ws_001”的数据
        Table selectTable = sensorTable
                .where($("id").isEqual("ws_001"))
                .select($("id"), $("ts"), $("vc"));

        //将selectTable写入文件系统
        //这里注意，如果写入的目标文件已存在，则结果不会输出到文件中
        tableEnv.connect(new FileSystem().path("out/result.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .createTemporaryTable("sensorOutPut");

        selectTable.executeInsert("sensorOutPut");

        //执行任务
        env.execute();
    }
}

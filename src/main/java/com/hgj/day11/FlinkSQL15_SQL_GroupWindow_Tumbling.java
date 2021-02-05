package com.hgj.day11;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL15_SQL_GroupWindow_Tumbling {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据创建流并转换每一行数据为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //流转化成表
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        //SQLAPI 实现滚动窗口
        Table result = tableEnv.sqlQuery("select " +
                "id," +
                "count(id)," +
                "TUMBLE_START(pt, INTERVAL '5' second) as wStart from " +
                table +
                " group by id,tumble(pt, INTERVAL '5' second)");

        //表转化成流输出
        tableEnv.toAppendStream(result, Row.class).print();

        //执行任务
        env.execute();

    }
}

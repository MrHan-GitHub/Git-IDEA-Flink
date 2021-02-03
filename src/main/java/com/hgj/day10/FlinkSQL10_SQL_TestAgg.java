package com.hgj.day10;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL10_SQL_TestAgg {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据，并转化成JavaBean
        SingleOutputStreamOperator<WaterSensor> WaterSensorDs = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(
                            split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2])
                    );
                });

        //将流进行注册
        tableEnv.createTemporaryView("sensor",WaterSensorDs);

        //使用SQL查询注册的表
        Table result = tableEnv.sqlQuery("select id,count(ts) ct,sum(vc) from sensor group by id");

        //将对象转化成流进行输出
        tableEnv.toRetractStream(result, Row.class).print();

        //执行任务
        env.execute();
    }
}

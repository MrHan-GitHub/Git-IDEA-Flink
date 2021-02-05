package com.hgj.day12;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSQL03_Function_UDF {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        //4.不注册函数直接使用
        //table.select(call(myLength.class,$("id"))).execute().print();

        //5.先注册再使用
        tableEnv.createTemporarySystemFunction("mylen",myLength.class);

        //TableAPI
        //table.select($("id"),call("myLen",$("id"))).execute().print();

        //SQLAPI
        tableEnv.sqlQuery("select id,myLen(id) from " + table).execute().print();

        //执行任务
        env.execute();

    }

    public static class myLength extends ScalarFunction{
        public int eval(String value) {
            return value.length();
        }
    }
}

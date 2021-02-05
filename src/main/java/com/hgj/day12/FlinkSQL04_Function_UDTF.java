package com.hgj.day12;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSQL04_Function_UDTF {
    public static void main(String[] args) {

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

        //先注册再使用
        tableEnv.createTemporarySystemFunction("split",Split.class);

        //TableAPI
        //table.joinLateral(call("split",$("id")))
        //        .select($("id"),$("word"))
        //        .execute()
        //        .print();

        //SQLAPI
        tableEnv.sqlQuery("select id,word from "+ table +" ,lateral table(split(id))").execute().print();

    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class Split extends TableFunction<Row>{
        public void eval(String value) {
            String[] s = value.split("_");
            for (String s1 : s) {
                collect(Row.of(s1));
            }
        }
    }
}

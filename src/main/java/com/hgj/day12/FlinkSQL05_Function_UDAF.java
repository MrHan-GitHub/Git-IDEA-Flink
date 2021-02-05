package com.hgj.day12;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSQL05_Function_UDAF {
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

        //4.注册
        tableEnv.createTemporarySystemFunction("myAvg",myAvg.class);

        //TableAPI
        //table.groupBy($("id"))
        //        .select($("id"),call("myAvg",$("vc")))
        //        .execute()
        //        .print();

        //SQL
        tableEnv.sqlQuery("select id,myAvg(vc) from " + table +" group by id").execute().print();


    }

    public static class myAvg extends AggregateFunction<Double,SumCount> {
        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }

        public void accumulate(SumCount acc,Integer vc){
            acc.setVcSum(acc.getVcSum() + vc);
            acc.setCount(acc.getCount() + 1);
        }

        @Override
        public Double getValue(SumCount sumCount) {
            return sumCount.getVcSum() * 1D / sumCount.getCount();
        }


    }
}

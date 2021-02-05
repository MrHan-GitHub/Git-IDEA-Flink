package com.hgj.day12;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 *  只有主表才会刷新。。。。
 *  再设置保留状态时，设置保留时间为10S
 *  先输入右表数据，再输入左表数据，中间过程中不停的输入左表数据，等待10S后右表的数据就会不存在，左表关联的就是null
 *  但是先输入左表数据，再输入右表数据，中间过程中不停得输入右表数据，等待10S后左表得数据一直存在，不会有null出现
 *
 */
public class FlinkSQL07_SQL_Join {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //默认值0 表示FlinkSQL中得状态永久保存
        //System.out.println(tableEnv.getConfig().getIdleStateRetention());

        //执行FlinkSQL状态保留10秒
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10L));

        SingleOutputStreamOperator<TableA> aDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new TableA(split[0], split[1]);
                });

        SingleOutputStreamOperator<TableB> bDS = env.socketTextStream("hadoop102", 8888)
                .map(data -> {
                    String[] split = data.split(",");
                    return new TableB(split[0], Integer.parseInt(split[1]));
                });

        //转化成临时表
        tableEnv.createTemporaryView("tableA",aDS);
        tableEnv.createTemporaryView("tableB",bDS);

        //双流JOIN
        tableEnv.sqlQuery("select * from tableA a left join tableB b on a.id = b.id")
                .execute()
                .print();

        //执行任务
        env.execute();

    }
}

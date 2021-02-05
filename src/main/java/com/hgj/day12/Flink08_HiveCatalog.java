package com.hgj.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink08_HiveCatalog {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建HiveCatalog
        HiveCatalog catalog = new HiveCatalog("myHive", "default", "input");

        //注册HiveCatalog
        tableEnv.registerCatalog("myHive",catalog);

        //使用HiveCatalog
        tableEnv.useCatalog("myHive");

        //执行查询，查询Hive中已经存在的表数据
        tableEnv.executeSql("select * from business2").print();

    }
}

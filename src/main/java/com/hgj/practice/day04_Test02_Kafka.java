package com.hgj.practice;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class day04_Test02_Kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"flink0122");

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("flink0122", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(new Tuple2<>(s1, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMap.keyBy(value -> value.f0).sum(1);
        sum.print();


        sum.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            private Connection conn;
            private PreparedStatement pre;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
                pre = conn.prepareStatement("INSERT INTO wordcount VALUES(?,?) ON DUPLICATE KEY UPDATE count=?;");
                super.open(parameters);
            }


            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                pre.setString(1, value.f0);
                pre.setInt(2, value.f1);
                pre.setInt(3, value.f1);
                pre.execute();
            }

            @Override
            public void close() throws Exception {
                pre.close();
                conn.close();
            }
        });

        env.execute();

    }
}

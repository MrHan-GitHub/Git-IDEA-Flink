package com.hgj.practice;

import com.hgj.been.wordToOne;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 *  实现使用FlinkSQL从Kafka读数据计算wordcount并将数据写到es
 *
 */
public class day10_Test01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取Kafka数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Flink02");

        DataStreamSource<String> kafkasource = env.addSource(new FlinkKafkaConsumer<String>("FlinkSQL_0203", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<wordToOne> wordToOneDS = kafkasource.flatMap(new FlatMapFunction<String, wordToOne>() {
            @Override
            public void flatMap(String value, Collector<wordToOne> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(new wordToOne(s, 1));
                }
            }
        });

        //将流传话成状态表
        Table wordToOnetable = tableEnv.fromDataStream(wordToOneDS);

        Table selectTable = wordToOnetable
                .groupBy($("word"))
                .select($("word"),
                        $("count").count().as("word_count"));

        //
        tableEnv.connect(new Elasticsearch()
                .index("wordtoone_sql")
                .documentType("_doc")
                .version("6")
                .host("hadoop102", 9200, "http")
                .bulkFlushMaxActions(1))
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("count", DataTypes.BIGINT()))
                .withFormat(new Json())
                .inUpsertMode()
                .createTemporaryTable("wordcount");

        selectTable.executeInsert("wordcount");

        env.execute();
    }
}

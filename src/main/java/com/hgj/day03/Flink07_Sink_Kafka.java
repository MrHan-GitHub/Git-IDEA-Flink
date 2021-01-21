package com.hgj.day03;

import com.alibaba.fastjson.JSON;
import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Flink07_Sink_Kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //转化成javabeen对象
        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink03_Tramsform_MaxBy.myMapFunction());

        //将数据转化成字符串写入kafka
        SingleOutputStreamOperator<String> map1 = map.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        //数据发送大kafka
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        map1.addSink(new FlinkKafkaProducer<String>("Flink0120",new SimpleStringSchema(),properties));

        //执行
        env.execute();
    }
}

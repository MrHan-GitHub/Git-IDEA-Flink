package com.hgj.day03;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Flink08_Sink_Redis {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //转化成javabeen对象
        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink03_Tramsform_MaxBy.myMapFunction());


        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        map.addSink(new RedisSink<>(conf,new mySinkReids()));

        env.execute();

    }

    public static class mySinkReids implements RedisMapper<WaterSensor>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"Sensor");
        }

        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(WaterSensor data) {
            return data.getVc().toString();
        }
    }
}

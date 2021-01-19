package com.hgj.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink10_Transform_Connect {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 8888);

        //将socketTextStream2转化成Int类型
        SingleOutputStreamOperator<Integer> intDS = socketTextStream2.map(value -> value.length());

        ConnectedStreams<String, Integer> connect = socketTextStream1.connect(intDS);

        SingleOutputStreamOperator<Object> map = connect.map(new myConnect());

        map.print();

        env.execute();


    }

    public static class myConnect implements CoMapFunction<String, Integer, Object> {

        @Override
        public Object map1(String value) throws Exception {
            return value;
        }

        @Override
        public Object map2(Integer value) throws Exception {
            return value;
        }
    }
}

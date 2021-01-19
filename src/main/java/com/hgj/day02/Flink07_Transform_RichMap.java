package com.hgj.day02;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 调用open方法看的是并行度，因为一个并行度会进到一个Slot中，所以会打印两次
 *
 * 调用close方法，一次open方法对调用两次close方法？？？？？？有待研究
 */
public class Flink07_Transform_RichMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> source = env.readTextFile("input/WaterSensor.txt");

        SingleOutputStreamOperator<WaterSensor> map = source.map(new myRichMapFunction());

        map.print();

        env.execute();

    }

    public static class myRichMapFunction extends RichMapFunction<String, WaterSensor>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open函数被调用");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("Close函数被掉用");
        }
    }
}

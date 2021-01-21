package com.hgj.day03;

import com.hgj.been.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink10_Sink_MySink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink03_Tramsform_MaxBy.myMapFunction());


        map.addSink(new mySink());

        env.execute();


    }

    public static class mySink extends RichSinkFunction<WaterSensor> {

        //声明链接
        private Connection conn;
        private PreparedStatement pre;

        //生命周期方法，创建链接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
            //conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
            pre = conn.prepareStatement("INSERT INTO `sensor` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?;");
            super.open(parameters);
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            //给占位符赋值
            pre.setString(1, value.getId());
            pre.setLong(2, value.getTs());
            pre.setInt(3, value.getVc());
            pre.setLong(4, value.getTs());
            pre.setInt(5, value.getVc());

            //执行操作
            pre.execute();

        }

        //生命周期方法，关闭连接
        @Override
        public void close() throws Exception {
            pre.close();
            conn.close();
        }
    }
}

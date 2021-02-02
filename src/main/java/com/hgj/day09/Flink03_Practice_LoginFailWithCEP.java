package com.hgj.day09;

import com.hgj.been.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink03_Practice_LoginFailWithCEP {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/LoginLog.csv");


        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<LoginEvent> loginEventDS = source.map(data -> {
            String[] split = data.split(",");
            return new LoginEvent(Long.parseLong(split[0]),
                    split[1],
                    split[2],
                    Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(loginEventWatermarkStrategy);

        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //定义模式
        //Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
        //        .where(new SimpleCondition<LoginEvent>() {
        //            @Override
        //            public boolean filter(LoginEvent value) throws Exception {
        //                return "fail".equals(value.getEnentType());
        //            }
        //        })
        //        .next("next")
        //        .where(new SimpleCondition<LoginEvent>() {
        //            @Override
        //            public boolean filter(LoginEvent value) throws Exception {
        //                return "fail".equals(value.getEnentType());
        //            }
        //        }).within(Time.seconds(2));

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEnentType());
                    }
                })
                .times(2) //默认使用的为宽松近邻
                .consecutive() //指定使用严格紧邻模式
                .within(Time.seconds(5));

        //将模式作用在流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //提取匹配上的事件
        SingleOutputStreamOperator<String> result = patternStream.select(new mySelectFunc());

        //打印结果
        result.print();

        //执行任务
        env.execute();
    }

    public static class mySelectFunc implements PatternSelectFunction<LoginEvent,String>{
        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
            //取出数据
            LoginEvent start = pattern.get("start").get(0);
            //LoginEvent next = pattern.get("next").get(0); //第一种方式
            LoginEvent next = pattern.get("start").get(1); //第二种方式
            return start.getUserId() + "在" + start.getEventTime()
                    + "到" + next.getEventTime() + "之间连续登录失败2次";
        }
    }
}

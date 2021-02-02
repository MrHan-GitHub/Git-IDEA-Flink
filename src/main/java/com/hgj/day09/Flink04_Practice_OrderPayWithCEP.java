package com.hgj.day09;

import com.hgj.been.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class Flink04_Practice_OrderPayWithCEP {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取文本数据，转化成javabean，提取时间戳生成watermark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //DataStreamSource<String> source = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9999);
                SingleOutputStreamOperator<OrderEvent> orderEventDS = source.map(data -> {
            String[] split = data.split(",");
            return new OrderEvent(Long.parseLong(split[0]),
                    split[1],
                    split[2],
                    Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        //分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //定义模式序列
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        //将模式序列作用在流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //提取正常匹配上的以及超时事件
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("No Pay") {
                                                                         },
                new OrderPayTimeOutFunc(),
                new OrderPaySelectFunc());

        //打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("No Pay") {
        }).print("Time Out");

        //执行任务
        env.execute();

    }

    public static class OrderPayTimeOutFunc implements PatternTimeoutFunction<OrderEvent, String> {
        @Override
        public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            //提取事件
            OrderEvent createEvent = pattern.get("start").get(0);
            //输出数据 测输出流
            return createEvent.getOrderId() + " 在 " + createEvent.getEventTime() + " 创建订单，并在 "
                    + timeoutTimestamp / 1000L + " 超时 ";
        }
    }

    public static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent,String>{
        @Override
        public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
            //提取事件
            OrderEvent createEvent = pattern.get("start").get(0);
            OrderEvent payEvent = pattern.get("follow").get(0);
            return createEvent.getOrderId() + " 在 " + createEvent.getEventTime() + " 创建订单，并在 "
                    + payEvent.getEventTime() + " 完成支付！！！";
        }
    }

    //public static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent, String> {
    //
    //    @Override
    //    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
    //
    //        //提取事件
    //        OrderEvent createEvent = pattern.get("start").get(0);
    //        OrderEvent payEvent = pattern.get("follow").get(0);
    //
    //        //输出结果  主流
    //        return createEvent.getOrderId() + "在 " + createEvent.getEventTime() +
    //                " 创建订单，并在 " + payEvent.getEventTime() + " 完成支付！！！";
    //    }
    //}

}

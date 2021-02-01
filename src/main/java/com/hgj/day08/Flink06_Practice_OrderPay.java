package com.hgj.day08;

import com.hgj.been.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 订单支付实时监控
 * 对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，
 * 网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 */
public class Flink06_Practice_OrderPay {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //读取文本数据，转化成javabeen，提取时间戳生成watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDs = env.readTextFile("input/OrderLog.csv")
                //SingleOutputStreamOperator<OrderEvent> orderEventDs = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new OrderEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        SingleOutputStreamOperator<String> result = orderEventDs.keyBy(OrderEvent::getOrderId).process(new OrderProcessFunc(15));

        result.print();
        result.getSideOutput(new OutputTag<String>("Payed TimeOut Or No Create") {
        }).print("No Create");
        result.getSideOutput(new OutputTag<String>("No Pay") {
        }).print("No Pay");

        env.execute();
    }

    public static class OrderProcessFunc extends KeyedProcessFunction<Long, OrderEvent, String> {

        private Integer interval;
        //声明状态
        private ValueState<OrderEvent> createState;
        private ValueState<Long> timerTsState;
        public OrderProcessFunc(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("create-state", OrderEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //判断当前数据中的类型
            if ("create".equals(value.getEventType())) {
                //更新状态
                createState.update(value);
                //注册定时器
                long ts = (value.getEventTime() + interval * 60) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);

                timerTsState.update(ts);

            } else if ("pay".equals(value.getEventType())) {
                //取出状态中的数据
                OrderEvent orderEvent = createState.value();

                if (orderEvent == null) {
                    //丢失了创建数据，或者超过15分钟才支付
                    ctx.output(new OutputTag<String>("Payed TimeOut Or No Create") {
                               },
                            value.getOrderId() + " Payed But No Create!");
                } else {
                    //结合写出
                    out.collect(value.getOrderId() +
                            " Create at:" +
                            orderEvent.getEventTime() +
                            " Payed at:" + value.getEventTime());

                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTsState.value());

                    //清空状态
                    createState.clear();
                    timerTsState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            OrderEvent orderEvent = createState.value();

            ctx.output(new OutputTag<String>("No Pay") {
                       },
                    orderEvent.getOrderId() + " Create But No Pay!");

            //清空状态
            createState.clear();
            timerTsState.clear();
        }
    }
}

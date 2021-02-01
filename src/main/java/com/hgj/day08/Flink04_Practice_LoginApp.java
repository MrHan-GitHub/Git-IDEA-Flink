package com.hgj.day08;

import com.google.common.collect.Lists;
import com.hgj.been.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 恶意登录监控
 * 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，
 * 就认为存在恶意登录的风险，输出相关的信息进行报警提示
 */

public class Flink04_Practice_LoginApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.读取文本数据,转换为JavaBean,提取时间戳生成Watermark
        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        //SingleOutputStreamOperator<LoginEvent> loginEventDS = env.socketTextStream("hadoop102", 9999)
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(loginEventWatermarkStrategy);

        //3.按照用户ID分组
        KeyedStream<LoginEvent, Long> KeyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //使用ProcessAPI，状态，定时器
        SingleOutputStreamOperator<String> result = KeyedStream.process(new LoginKeyedProcessFunc(2, 2));

        result.print();

        env.execute();

    }

    public static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {

        //定义属性信息
        private Integer ts;
        private Integer count;
        //声明状态
        private ListState<LoginEvent> loginEventListStatel;
        private ValueState<Long> valueState;
        public LoginKeyedProcessFunc(Integer ts, Integer count) {
            this.ts = ts;
            this.count = count;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListStatel = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));

        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListStatel.get().iterator();
            Long timerTs = valueState.value();

            //取出事件类型
            String enentType = value.getEnentType();

            //判断是否为第一条数据，则需要注册定时器
            if ("fail".equals(enentType)) {
                if (!iterator.hasNext()) { //为第一条失败数据
                    //注册定时器
                    long curTs = ctx.timerService().currentWatermark() + this.ts * 1000L;
                    ctx.timerService().registerEventTimeTimer(curTs);
                    //更新时间状态
                    valueState.update(curTs);
                }
                //将当前的失败数据加入状态
                loginEventListStatel.add(value);
            } else {
                //说明已经注册过定时器
                if (timerTs != null) {
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                }

                //成功数据，清空List
                loginEventListStatel.clear();
                valueState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListStatel.get().iterator();
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(iterator);

            //判断连续失败的次数
            if (loginEvents.size() >= count) {
                LoginEvent first = loginEvents.get(0);
                LoginEvent last = loginEvents.get(loginEvents.size() - 1);

                out.collect(first.getUserId() +
                        "用户在" +
                        first.getEventTime() +
                        "到" +
                        last.getEventTime() +
                        "之间，连续登录失败了" +
                        loginEvents.size() + "次！");
            }
            //清空状态
            loginEventListStatel.clear();
            valueState.clear();
        }
    }
}

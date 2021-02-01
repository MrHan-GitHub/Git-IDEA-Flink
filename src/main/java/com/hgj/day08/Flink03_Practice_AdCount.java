package com.hgj.day08;

import com.hgj.been.AdCount;
import com.hgj.been.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 黑名单做过滤
 */
public class Flink03_Practice_AdCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");

        WatermarkStrategy<AdsClickLog> adsClickLogWatermarkStrategy = WatermarkStrategy.<AdsClickLog>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                    @Override
                    public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<AdsClickLog> adsClickLogDS = source.map(data -> {
            String[] split = data.split(",");
            return new AdsClickLog(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    split[2],
                    split[3],
                    Long.parseLong(split[4]));
        }).assignTimestampsAndWatermarks(adsClickLogWatermarkStrategy);

        //根据黑名单做过滤
        SingleOutputStreamOperator<AdsClickLog> filter = adsClickLogDS.keyBy(data -> data.getUserId() + "_" + data.getAdId())
                .process(new BlackListProcessFunc(100L));

        //按照省份分组
        KeyedStream<AdsClickLog, String> proviceKeyedStream = filter.keyBy(AdsClickLog::getProvince);

        //开窗聚合
        SingleOutputStreamOperator<AdCount> result = proviceKeyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdCountAggFunc(), new AdCountWindowFunc());

        //打印结果
        result.print();
        filter.getSideOutput(new OutputTag<String>("BlackList"){}).print();

        env.execute();

    }

    public static class BlackListProcessFunc extends KeyedProcessFunction<String, AdsClickLog, AdsClickLog> {

        //定义最大点击次数属性
        private Long maxClickCount;

        //声明状态
        private ValueState<Long> countState;
        private ValueState<Boolean> isSendState;

        public BlackListProcessFunc(Long maxClickCount) {
            this.maxClickCount = maxClickCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("boolean-state", Boolean.class));
        }

        @Override
        public void processElement(AdsClickLog value, Context ctx, Collector<AdsClickLog> out) throws Exception {
            //取出状态中的数据
            Long count = countState.value();
            Boolean isSend = isSendState.value();

            //判断是第一条数据
            if (count == null) {
                //赋值为1
                countState.update(1L);
                //注册第二天凌晨的定时器，用于清空状态
                long ts = (value.getTimestamp() / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - 8 * 60 * 60 * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
            } else {
                //非第一条数据
                count = count + 1;
                //更新状态
                countState.update(count);
                //判断是否超过阈值
                if (count >= maxClickCount) {
                    if (isSend == null) {
                        //报警信息进测输出流
                        ctx.output(new OutputTag<String>("BlackList") {
                                   },
                                value.getUserId() + "点击了" + value.getAdId() + "超过了" + maxClickCount + "次,存在恶意点击广告行为,报警！");
                        //更新黑名单状态
                        isSendState.update(true);
                    }
                    //之后就不用报警了,直接返回就可以
                    return;
                }
            }
            //输出数据
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdsClickLog> out) throws Exception {
            isSendState.clear();
            countState.clear();
        }
    }
    public static class AdCountAggFunc implements AggregateFunction<AdsClickLog,Integer,Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(AdsClickLog value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class AdCountWindowFunc implements WindowFunction<Integer, AdCount,String, TimeWindow>{
        @Override
        public void apply(String province, TimeWindow window, Iterable<Integer> input, Collector<AdCount> out) throws Exception {
            Integer count = input.iterator().next();
            out.collect(new AdCount(province,window.getEnd(),count));
        }
    }

}

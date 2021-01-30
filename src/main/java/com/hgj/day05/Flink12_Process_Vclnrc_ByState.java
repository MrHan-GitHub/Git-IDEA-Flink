package com.hgj.day05;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink12_Process_Vclnrc_ByState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        KeyedStream<WaterSensor, String> keyBy = map.keyBy(value -> value.getId());

        //使用ProcessFunction实现连续时间内水位不下降，则报警，且报警信息输出到测输出流
        SingleOutputStreamOperator<WaterSensor> result = keyBy.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            //定义状态
            private ValueState<Integer> vcState;
            private ValueState<Long> tsState;

            //初始化状态

            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state", Integer.class, Integer.MIN_VALUE));
                tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
                super.open(parameters);
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //取出状态数据
                Integer lastvc = vcState.value();
                Long timerTs = tsState.value();

                //取出当前数据中的水位线
                Integer curvc = value.getVc();

                //当前水位线与上一次比较
                if (lastvc > curvc && timerTs == null) {
                    //注册定时器
                    long ts = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                } else if (curvc < lastvc && timerTs != null) {
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    tsState.clear();
                }
                //更新上一次vc
                lastvc = curvc;
                //输出数据
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("SideOut") {
                           },
                        ctx.getCurrentKey() + "连续10s内水位没有下降");
                //清空定时器时间状态
                tsState.clear();
            }
        });

        //打印数据
        result.print("主流");
        result.getSideOutput(new OutputTag<String>("SideOut"));

        //执行任务
        env.execute();

    }
}

package com.hgj.day05;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink09_Process_OnTimer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        map.keyBy(WaterSensor::getTs).process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //获取当前数据的处理时间
                long ts = ctx.timerService().currentProcessingTime();
                System.out.println(ts);

                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts + 1000L);

                //输出数据
                out.collect(value);
            }

            //注册的定时器响起，触发动作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("定时器触发" + timestamp);
                long ts = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(ts + 1000L);

            }
        }).print();

        //执行任务
        env.execute();

    }
}

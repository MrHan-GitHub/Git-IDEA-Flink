package com.hgj.day05;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink10_Process_Vclnrc {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = map.keyBy(WaterSensor::getId);

        //使用Processfunction实现连续时间内水位不下降则报警，且将报警信息输出到测输出流
        SingleOutputStreamOperator<WaterSensor> result = waterSensorStringKeyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private Integer lastVc = Integer.MIN_VALUE;
            private Long timerTS = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //取出水位线
                Integer vc = value.getVc();

                //将水位线与上次的值进行比较
                if (vc >= lastVc && timerTS == Long.MIN_VALUE) {
                    //注册定时器
                    long ts = ctx.timerService().currentProcessingTime() + 10000L;
                    System.out.println("注册定时器" + ts);
                    ctx.timerService().registerProcessingTimeTimer(ts);

                    //更新上一次的水位线，定时器的时间戳
                    timerTS = ts;
                } else if (vc < lastVc) {
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTS);
                    System.out.println("删除定时器" + timerTS);
                    timerTS = Long.MIN_VALUE;
                }

                lastVc = vc;

                //输出数据
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("SideOut") {
                           },
                        ctx.getCurrentKey() + "连续10s水位线没有下降");
                timerTS = Long.MIN_VALUE;
                //注册定时器
                long ts = ctx.timerService().currentProcessingTime() + 10000L;
                System.out.println("注册定时器" + ts);
                ctx.timerService().registerProcessingTimeTimer(ts);
                //更新上一次的ts
                timerTS = ts;
            }
        });

        //打印数据
        result.print("主流");
        result.getSideOutput(new OutputTag<String>("SideOut"){}).print("SideOutPut");

        //执行任务
        env.execute();
    }
}

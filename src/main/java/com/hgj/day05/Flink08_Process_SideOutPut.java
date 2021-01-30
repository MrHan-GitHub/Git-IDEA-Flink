package com.hgj.day05;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink08_Process_SideOutPut {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        //使用ProcessFunction将数据分流
        SingleOutputStreamOperator<WaterSensor> process = map.process(new mySplitFunc());

        process.print("主流");
        DataStream<Tuple2<String, Integer>> sideOutput = process.getSideOutput(new OutputTag<Tuple2<String, Integer>>("Side") {
        });
        sideOutput.print("Side");

        env.execute();
    }

    public static class mySplitFunc extends ProcessFunction<WaterSensor,WaterSensor>{
        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            //取出水位线
            Integer vc = value.getVc();

            //根据水位线高低，分流
            if (vc >= 30) {
                //将数据输出到主流
                out.collect(value);
            } else {
                //将数据输出到测输出流
                ctx.output(new OutputTag<Tuple2<String,Integer>>("Side"){
                },
                        new Tuple2<>(value.getId(),vc));
            }
        }
    }
}

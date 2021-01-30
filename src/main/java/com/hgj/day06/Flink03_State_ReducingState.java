package com.hgj.day06;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * 使用状态编程的方式实现累加传感器的水位线
 *
 */
public class Flink03_State_ReducingState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
            }
        }).keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

                    //定义状态
                    private ReducingState<WaterSensor> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reduce-state", new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                return new WaterSensor(value1.getId(),value2.getTs(),value1.getVc() + value2.getVc());
                            }
                        },WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        //将当数据聚合进状态
                        reducingState.add(value);
                        //取出状态中的数据
                        WaterSensor waterSensor = reducingState.get();
                        //输出数据
                        out.collect(waterSensor);
                    }
                }).print();

        env.execute();


    }
}

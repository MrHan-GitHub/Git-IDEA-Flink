package com.hgj.day06;

import com.hgj.been.AvgVc;
import com.hgj.been.WaterSensor;
import com.hgj.been.WaterSensor2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用状态编程方式实现平均水位
 *
 */
public class Flink04_State_AggState {
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
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor2>() {

                    //定义状态
                    private AggregatingState<Integer,Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, AvgVc, Double>("agg-state", new AggregateFunction<Integer, AvgVc, Double>() {
                            @Override
                            public AvgVc createAccumulator() {
                                return new AvgVc(0,0);
                            }

                            @Override
                            public AvgVc add(Integer value, AvgVc accumulator) {
                                return new AvgVc(accumulator.getVcSum() + value,
                                        accumulator.getCount() + 1);
                            }

                            @Override
                            public Double getResult(AvgVc accumulator) {
                                return accumulator.getVcSum() * 1D / accumulator.getCount();
                            }

                            @Override
                            public AvgVc merge(AvgVc a, AvgVc b) {
                                return new AvgVc(a.getVcSum() + b.getVcSum(),a.getCount() + b.getCount());
                            }
                        },AvgVc.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor2> out) throws Exception {
                        //将当前数据写入状态
                        aggregatingState.add(value.getVc());
                        //取出状态中的数据
                        Double avgVc = aggregatingState.get();
                        //输出数据据
                        out.collect(new WaterSensor2(value.getId(),value.getTs(),avgVc));
                    }
                }).print();

        //执行任务
        env.execute();

    }
}

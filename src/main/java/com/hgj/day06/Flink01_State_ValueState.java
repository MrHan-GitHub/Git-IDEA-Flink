package com.hgj.day06;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_State_ValueState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> WaterSensorDs = source.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = WaterSensorDs.keyBy(WaterSensor::getId);

        keyedStream.flatMap(new RichFlatMapFunction<WaterSensor, String>() {

            //定义状态
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state",Integer.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                //获取状态中的数据
                Integer lastvc = valueState.value();

                //更新状态
                valueState.update(value.getVc());

                //当上一次水位不为NULL且出现跳变的时候进行报警
                if (lastvc != null && Math.abs(value.getVc() - lastvc) > 10) {
                    out.collect(value.getId() + "出现水位线跳变");
                }
            }
        }).print();

        //执行任务
        env.execute();

    }
}

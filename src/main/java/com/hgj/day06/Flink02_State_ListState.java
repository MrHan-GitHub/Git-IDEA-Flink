package com.hgj.day06;

import com.google.common.collect.Lists;
import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 使用ListState实现每隔传感器最高的三个水位线
 *
 */
public class Flink02_State_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> WaterSensorDs = source.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        WaterSensorDs.keyBy(WaterSensor::getId).map(new RichMapFunction<WaterSensor, List<WaterSensor>>() {

            //定义状态
            private ListState<WaterSensor> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor>("list-state",WaterSensor.class));
            }

            @Override
            public List<WaterSensor> map(WaterSensor value) throws Exception {
                //将当前数据加入状态
                listState.add(value);

                //取出状态中的数据并排序
                ArrayList<WaterSensor> waterSensors = Lists.newArrayList(listState.get().iterator());
                waterSensors.sort((o1, o2) -> o2.getVc() - o1.getVc());

                //判断当前数据是否超过3条，如果超过，则删除最后一条
                if (waterSensors.size() > 3 ) {
                    waterSensors.remove(3);
                }

                //更新状态
                listState.update(waterSensors);

                //返回数据
                return waterSensors;
            }
        }).print();

        env.execute();


    }
}

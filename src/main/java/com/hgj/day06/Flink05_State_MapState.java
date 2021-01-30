package com.hgj.day06;


import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用状态编程方式实现水位线去重
 *
 */
public class Flink05_State_MapState {
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
                    private MapState<Integer,String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("map-state",Integer.class,String.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        //取出当前数据中的水位线
                        Integer vc = value.getVc();
                        //判断状态中是否包含当前水位线的值
                        if (!mapState.contains(vc)) {
                            out.collect(value);
                            mapState.put(vc,"aa");
                        }
                    }
                }).print();

        env.execute();

    }
}

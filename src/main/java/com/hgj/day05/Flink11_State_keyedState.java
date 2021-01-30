package com.hgj.day05;

import akka.stream.impl.ReducerState;
import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * 状态编程
 */

public class Flink11_State_keyedState {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据并转化成javabeen对象
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink01_Window_EventTimeTumbling.myMapFunc());

        //按照传感器分组
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = map.keyBy(WaterSensor::getId);

        //演示状态的使用
        SingleOutputStreamOperator<WaterSensor> result = waterSensorStringKeyedStream.process(new myStateProcessFunc());

        //打印
        result.print();

        //执行任务
        env.execute();

    }

    public static class myStateProcessFunc extends KeyedProcessFunction<String,WaterSensor,WaterSensor>{

        //定义状态
        private ValueState<Long> valueState;
        private ListState<Long> listState;
        private MapState<String,Long> mapState;
        private ReducingState<WaterSensor> reducingState;
        private AggregatingState<WaterSensor,WaterSensor> aggregatingState;

        //初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state",Long.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("List-state",Long.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("Map-state",String.class,Long.class));
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reduce-state", new ReduceFunction<WaterSensor>() {
                @Override
                public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                    return new WaterSensor(value1.getId(),value2.getTs(),Math.max(value1.getVc(),value2.getVc()));
                }
            },WaterSensor.class));
            //aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Tuple2<String,Integer>, WaterSensor>());
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            //状态的使用
            //value的状态
            Long value1 = valueState.value();
            valueState.update(122L);
            valueState.clear();

            //list的状态
            Iterable<Long> longs = listState.get();
            listState.add(122L);
            listState.clear();
            listState.update(new ArrayList<>());

            //Map的状态
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            Long aLong = mapState.get("");
            mapState.contains("");
            mapState.put("",122L);
            mapState.putAll(new HashMap<>());
            mapState.remove("");
            mapState.clear();

            //Reduce的状态
            WaterSensor waterSensor = reducingState.get();
            reducingState.add(new WaterSensor());
            reducingState.clear();

            //Agg的状态
            aggregatingState.add(value);
            WaterSensor waterSensor1 = aggregatingState.get();
            aggregatingState.clear();
        }
    }
}

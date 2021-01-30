package com.hgj.day06;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Iterator;

/**
 * 算子状态
 * 统计元素的个数
 *
 */
public class Flink06_State_ListState_OP {
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
                .map(new myMapFunc())
                .print();

        env.execute();
    }

    public static class myMapFunc implements MapFunction<WaterSensor,Integer>, CheckpointedFunction{

        //定义状态
        private ListState<Integer> listState;
        private Integer count = 0;

        @Override
        public Integer map(WaterSensor value) throws Exception {
            count ++;
            return count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            listState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));

            //有状态，所以每一次初始化的时候先要读取状态中的值，为了恢数据
            Iterator<Integer> iterator = listState.get().iterator();

            while (iterator.hasNext()) {
                count += iterator.next();
            }
        }
    }

}

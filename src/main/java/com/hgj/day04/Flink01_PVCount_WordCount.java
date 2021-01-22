package com.hgj.day04;

import com.hgj.been.UserBehavior;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_PVCount_WordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        //转化成UserBehavior对象
        SingleOutputStreamOperator<UserBehavior> map = source.map(new myUserBehavior());

        //过滤出不是pv的操作
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new myfliterFunction());

        //转化成 (pv,1) 之后方便统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> map1 = filter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>(value.getBehavior(), 1);
            }
        });

        //求和,但是sum()函数只能跟在keyBy后面，所以我们还要对自身进行keyBy操作
        map1.keyBy(value -> value.f0).sum(1).print();

        env.execute();

    }

    public static class myUserBehavior implements MapFunction<String,UserBehavior>{
        @Override
        public UserBehavior map(String value) throws Exception {
            String[] split = value.split(",");
            return new UserBehavior(
                    Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4])
            );
        }
    }

    public static class myfliterFunction implements FilterFunction<UserBehavior>{

        @Override
        public boolean filter(UserBehavior value) throws Exception {
            if ("pv".equals(value.getBehavior())){
                return true;
            } else {
                return false;
            }
        }
    }
}

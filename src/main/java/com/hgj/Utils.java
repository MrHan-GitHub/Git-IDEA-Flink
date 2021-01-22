package com.hgj;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Utils {
    public static class myFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] s = value.split(" ");
            for (String s1 : s) {
                out.collect(new Tuple2<String, Integer>(s1,1));
            }
        }
    }
}

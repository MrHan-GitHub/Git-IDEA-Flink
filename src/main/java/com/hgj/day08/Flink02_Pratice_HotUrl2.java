package com.hgj.day08;

import com.google.common.collect.Lists;
import com.hgj.been.ApacheLog;
import com.hgj.been.UrlCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class Flink02_Pratice_HotUrl2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //DataStreamSource<String> source = env.readTextFile("input/Apache.log");
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9999);

        WatermarkStrategy<ApacheLog> apacheLogWatermarkStrategy = WatermarkStrategy.<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                    @Override
                    public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                        return element.getTs();
                    }
                });

        SingleOutputStreamOperator<ApacheLog> apacheLogDS = source.map(data -> {
            String[] split = data.split(" ");
            SimpleDateFormat sdf = new SimpleDateFormat("dd/mm/yyyy:HH:mm:dd");
            return new ApacheLog(split[0],
                    split[1],
                    sdf.parse(split[3]).getTime(),
                    split[5],
                    split[6]);
        }).filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(apacheLogWatermarkStrategy);

        SingleOutputStreamOperator<Tuple2<String, Integer>> UrlToOneDS = apacheLogDS.map(new MapFunction<ApacheLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(ApacheLog value) throws Exception {
                return new Tuple2<>(value.getUrl(), 1);
            }
        });

        SingleOutputStreamOperator<UrlCount> urlCountByWindowDS = UrlToOneDS.keyBy(value -> value.f0).window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("SideOutPut") {
                })
                .aggregate(new HotUrlAggFunc(), new HotUrlWindowFunc());

        SingleOutputStreamOperator<String> result = urlCountByWindowDS.keyBy(UrlCount::getWindowEnd).process(new HotUrlProcessFunc(5));

        result.print();

        env.execute();

    }

    public static class HotUrlAggFunc implements AggregateFunction<Tuple2<String, Integer>,Integer,Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class HotUrlWindowFunc implements WindowFunction<Integer,UrlCount,String,TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<UrlCount> out) throws Exception {
            Integer count = input.iterator().next();
            out.collect(new UrlCount(s,window.getEnd(),count));
        }
    }



    public static class HotUrlProcessFunc extends KeyedProcessFunction<Long,UrlCount,String> {

        private Integer TopSize;
        //声明状态
        private MapState<String,UrlCount> mapState;

        public HotUrlProcessFunc(Integer topSize) {
            TopSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("map-state",String.class,UrlCount.class));
        }

        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
            //将当前数据放入状态
            mapState.put(value.getUrl(),value);

            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);

            //注册定时器，在窗口真正关闭的时候用于清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 61001L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (timestamp == ctx.getCurrentKey() + 61001L) {
                return;
            }

            //取出状态中的数据
            Iterator<Map.Entry<String, UrlCount>> iterator = mapState.iterator();
            ArrayList<Map.Entry<String, UrlCount>> entries = Lists.newArrayList(iterator);

            //排序
            entries.sort((o1, o2) -> o2.getValue().getCount() - o1.getValue().getCount());

            //去TopN数据
            StringBuffer sb = new StringBuffer();
            sb.append("===============")
                    .append(new Timestamp(timestamp - 1L))
                    .append("================")
                    .append("\n");

            for (int i = 0; i < Math.min(TopSize, entries.size()); i++) {
                UrlCount urlCount = entries.get(i).getValue();

                sb.append("Top").append(i + 1);
                sb.append(" Url:").append(urlCount.getUrl());
                sb.append(" Count:").append(urlCount.getCount());
                sb.append("\n");
            }

            sb.append("===============")
                    .append(new Timestamp(timestamp - 1L))
                    .append("================")
                    .append("\n");

            //输出数据
            out.collect(sb.toString());

            //休息一会
            Thread.sleep(2000);
        }
    }
}

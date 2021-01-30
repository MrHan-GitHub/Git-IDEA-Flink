package com.hgj.day07;

import com.hgj.been.UserBehavior;
import com.hgj.been.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

public class Flink05_Practice_UserVisitor_Window2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = source.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(
                    Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4])
            );
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        KeyedStream<UserBehavior, String> userBehaviorStringKeyedStream = userBehaviorDS.keyBy(UserBehavior::getBehavior);

        WindowedStream<UserBehavior, String, TimeWindow> window = userBehaviorStringKeyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        //使用布隆过滤器，自定义触发器：来一条计算一条(访问Redis一次)
        SingleOutputStreamOperator<UserVisitorCount> result = window.trigger(new MyTigger())
                .process(new UserVisitorWindowFunc());

        //打印
        result.print();

        //执行任务
        env.execute();
    }

    //自定义触发器：来一条计算一条(访问Redis一次)
    public static class MyTigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UserVisitorWindowFunc extends ProcessWindowFunction<UserBehavior,UserVisitorCount,String,TimeWindow> {

        //声明Redis链接
        private Jedis jedis;

        //声明布隆过滤器
        private MyBloomFilter myBoomFilter;

        //声明每个窗口的总人数的key
        private String hourUvCountKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102",6379);
            hourUvCountKey = "HourUv";
            myBoomFilter = new MyBloomFilter(1 << 30);
        }

        @Override
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
            //1.取出数据
            UserBehavior userBehavior = elements.iterator().next();

            //2.提取窗口信息
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            //3.定义当前窗口的BitMap key
            String bitMapKey = "BitMap_" + windowEnd;

            //4.查询当前UID是否已经存在与当前的bitMap中
            long offset = myBoomFilter.getOffset(userBehavior.getUserId().toString());
            Boolean exist = jedis.getbit(bitMapKey, offset);

            //5.根据数据是否存在决定下一步操作
            if (!exist){
                //将对应Offset位置改变成1
                jedis.setbit(bitMapKey,offset,true);
                //累加当前窗口的综合
                jedis.hincrBy(hourUvCountKey,windowEnd,1);
            }

            //6.输出数据
            String hget = jedis.hget(hourUvCountKey, windowEnd);
            out.collect(new UserVisitorCount("UV",windowEnd,Integer.parseInt(hget)));
        }


    }

    //自定义布隆过滤器
    public static class MyBloomFilter{
        //定义布隆过滤器容量，最好传入2的整数次幂数据
        private long cap;

        public MyBloomFilter(long cap) {
            this.cap = cap;
        }

        //传入一个字符串，获取BitMap中的位置
        public long getOffset(String value){
            long result = 0;

            for (char c : value.toCharArray()) {
                result += result * 31 + c;
            }

            //取模
            return result & (cap - 1);
        }
    }
}

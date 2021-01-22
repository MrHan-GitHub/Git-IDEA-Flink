package com.hgj.day04;

import com.hgj.been.OrderEvent;
import com.hgj.been.TxEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class Flink07_Project_Order {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);

        //读取数据
        DataStreamSource<String> ReceiptLogDS = env.readTextFile("input/ReceiptLog.csv");
        DataStreamSource<String> orderLogDS = env.readTextFile("input/OrderLog.csv");

        //转化成javaBeen对象
        SingleOutputStreamOperator<TxEvent> TxEvent = ReceiptLogDS.map(new myReceiptLogMapFunction());
        SingleOutputStreamOperator<OrderEvent> OrderLog = orderLogDS.map(new myOrderLogMapFunction());

        //过滤出OrderLog中时间类型是create的，就是创建订单(因为创建的订单不一定已经支付，但是pay的订单肯定是已经支付的)，
        //只要在规定时间内，create订单的Id能够在TxEvent中找得到相应的id就说明创建的订单已经被支付
        SingleOutputStreamOperator<OrderEvent> payOrderLog = OrderLog.filter(new myFilterFunction());

        //将两个流进行connect  双流join可以采用union和connect
        //但是union之前两个流的类型必须是一样，connect可以不一样
        ConnectedStreams<OrderEvent, TxEvent> connect = payOrderLog.connect(TxEvent);

        //这里可以采用
        connect.keyBy("txId","txId").process(new myProcess()).print();

        env.execute();

    }

    public static class myReceiptLogMapFunction implements MapFunction<String, TxEvent> {

        @Override
        public TxEvent map(String value) throws Exception {
            String[] split = value.split(",");
            return new TxEvent(
                    split[0],
                    split[1],
                    Long.parseLong(split[2])
            );
        }
    }

    public static class myOrderLogMapFunction implements MapFunction<String, OrderEvent> {
        @Override
        public OrderEvent map(String value) throws Exception {
            String[] split = value.split(",");
            return new OrderEvent(
                    Long.parseLong(split[0]),
                    split[1],
                    split[2],
                    Long.parseLong(split[3])
            );
        }
    }

    public static class myFilterFunction implements FilterFunction<OrderEvent> {
        @Override
        public boolean filter(OrderEvent value) throws Exception {
            if ("pay".equals(value.getEventType())) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static class myProcess extends CoProcessFunction<OrderEvent,TxEvent,String> {

        //存储 txId -> OrderEvent
        HashMap<String, OrderEvent> OrderMap = new HashMap<>();
        //存储 txId -> TxEvent
        HashMap<String, TxEvent> TxMap = new HashMap<>();

        //createOrderLog先来，要去拿着自身的id去TxMap找，找到了就收集走并且把TxMap中此条信息移除
        //否则把词条信息存储进OrderMap中。
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            if (TxMap.containsKey(value.getTxId())){
                out.collect("订单：" + value + "支付成功");
                TxMap.remove(value.getTxId());
            } else {
                OrderMap.put(value.getTxId(),value);
            }
        }
        //TxEvent先来，要拿着自身的id去OrderMap中找，找到了就收集走并且把OrderMap中此条信息移除掉
        //否则把此条信息存储进TxMap中。

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
            if (OrderMap.containsKey(value.getTxId())){
                out.collect("订单" + value + "对账成功");
                OrderMap.remove(value.getTxId());
            } else {
                TxMap.put(value.getTxId(),value);
            }
        }
    }

}

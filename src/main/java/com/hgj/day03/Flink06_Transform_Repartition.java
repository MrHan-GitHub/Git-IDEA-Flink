package com.hgj.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_Repartition {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = source.map(x -> x).setParallelism(2);

        map.print("Map").setParallelism(2);
        //map.keyBy(x -> x).print("keyBy"); //是按照自己进行分组，所以相同的会进入到一个Slot中
        //map.shuffle().print("shuffle"); //随机
        map.rebalance().print("rebalance"); //不管前者的并行度，会将来的数据轮询的发送给后面的Slot中
        map.rescale().print("rescale");  //会让后者的并行度均分给前者并行度，之后前者来数据之后，会轮询的将数据发送到给自己分配的Slot中
        //map.forward().print("forward"); //必须要求前后的并行度一致，读取端口数据的并行度为1，但是后面的并行度为4，程序会报错
        //map.global().print("global"); //都会进到1号Slot中，因为源码中返回值时return 0，是一个固定的值
        //map.broadcast().print("broadcast"); //会广播到所有的并行度中，并行度有多少，南无就会广播多少次

        System.out.println();

        env.execute();

    }
}

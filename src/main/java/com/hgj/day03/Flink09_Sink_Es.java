package com.hgj.day03;

import com.hgj.been.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import sun.misc.Request;

import java.util.ArrayList;
import java.util.HashMap;

public class Flink09_Sink_Es {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //DataStreamSource<String> source = env.readTextFile("input/WaterSensor.txt");

        SingleOutputStreamOperator<WaterSensor> map = source.map(new Flink03_Tramsform_MaxBy.myMapFunction());


        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new mySinkEs()
        );

        waterSensorBuilder.setBulkFlushMaxActions(1);

        ElasticsearchSink<WaterSensor> build = waterSensorBuilder.build();

        map.addSink(build);

        env.execute();

    }

    public static class mySinkEs implements ElasticsearchSinkFunction<WaterSensor> {

        @Override
        public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {

            HashMap<String, String> source = new HashMap<>();
            source.put("ts", element.getTs().toString());
            source.put("vc", element.getVc().toString());

            //创建Index请求
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor1")
                    .type("_doc")
                    .id(element.getId())
                    .source(source);

            //写入ES
            indexer.add(indexRequest);
        }
    }
}

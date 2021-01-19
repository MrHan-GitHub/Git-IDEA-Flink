package com.hgj.day02;

import com.hgj.been.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Flink05_Source_MySource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.addSource(new myKafkaFunction("hadoop102", 9999));

        waterSensorDataStreamSource.print();

        env.execute();

    }

    public static class myKafkaFunction implements SourceFunction<WaterSensor> {

        Socket socket = null;
        BufferedReader bufferedReader = null;
        private Boolean running = true;
        private String host;
        private int port;

        public myKafkaFunction(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            socket = new Socket(host, port);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            String line = bufferedReader.readLine();
            while (running && line != null) {
                String[] split = line.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                ctx.collect(waterSensor);
                line = bufferedReader.readLine();
            }
        }

        @Override
        public void cancel() {
            running = false;
            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

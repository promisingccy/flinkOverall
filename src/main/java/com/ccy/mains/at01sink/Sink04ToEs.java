package com.ccy.mains.at01sink;

import com.alibaba.fastjson.JSONObject;
import com.ccy.entity.Event;
import com.ccy.sink.EsSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Sink04ToEs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<JSONObject> stream = env.fromElements(
                new Event("Mary", "./home", System.currentTimeMillis()).toJson(),
                new Event("Bob", "./cart", System.currentTimeMillis() - 2000L).toJson(),
                new Event("Alice", "./prod?id=100", System.currentTimeMillis()-3000L).toJson(),
                new Event("Alice", "./prod?id=200", System.currentTimeMillis()-3500L).toJson(),
                new Event("Bob", "./prod?id=2", System.currentTimeMillis()-2500L).toJson(),
                new Event("Alice", "./prod?id=300", System.currentTimeMillis()-3600L).toJson(),
                new Event("Bob", "./home", System.currentTimeMillis()-3000L).toJson(),
                new Event("Bob", "./prod?id=1", System.currentTimeMillis()-2300L).toJson(),
                new Event("Bob", "./prod?id=3", System.currentTimeMillis()-3300L).toJson());

        stream.addSink(EsSink.sinkEs("clicks").build());

        env.execute();
    }
}

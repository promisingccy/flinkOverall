package com.ccy.mains.at02trans;

import com.ccy.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Trans01Map {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 传入匿名类，实现MapFunction
//        stream.map(new MapFunction<Event, String>() {
//            @Override
//            public String map(Event e) throws Exception {
//                return e.user;
//            }
//        });

        // 传入MapFunction的实现类
        stream.map(new UserExtractor()).print();

        env.execute();
    }
    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}


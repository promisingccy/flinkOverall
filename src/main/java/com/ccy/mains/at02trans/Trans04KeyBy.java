package com.ccy.mains.at02trans;

import com.ccy.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Trans04KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 3000L),
                new Event("Mary", "./home1", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Bob", "./cart", 4000L)
        );

        //有两个类型 <Event, String>
        //<stream的类型, key的类型>
        KeyedStream<Event, String> keyBy = stream.keyBy(e -> e.user);

        //Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:03.0} //Mary-08:00:03 min
        //Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0} //Mary-08:00:01 min
        //Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0} //Bob-08:00:02 min
        //Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0} //Bob-08:00:04 非min
        keyBy.min("timestamp").print();

        env.execute();
    }


}


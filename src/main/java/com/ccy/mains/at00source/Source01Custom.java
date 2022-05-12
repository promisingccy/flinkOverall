package com.ccy.mains.at00source;

import com.ccy.entity.Event;
import com.ccy.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source01Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //有了自定义的source function，调用addSource方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("Source01Custom");

        env.execute();
    }
}


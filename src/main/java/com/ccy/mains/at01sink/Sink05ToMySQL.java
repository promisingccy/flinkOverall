package com.ccy.mains.at01sink;

import com.ccy.entity.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Sink05ToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //create table clicks(`user` varchar(20) not null,`url` varchar(100) not null);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        stream.addSink(
            JdbcSink.sink(
                "INSERT INTO clicks (user, url) VALUES (?, ?)",
                (statement, r) -> {
                    statement.setString(1, r.user);
                    statement.setString(2, r.url);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://10.0.81.118:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("xxx")
                        .build()
            )
        );
        env.execute();
    }
}


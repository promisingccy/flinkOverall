package com.ccy.mains.at04window;

import com.ccy.entity.Event;
import com.ccy.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Window01Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    })
            );

        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(Event value) throws Exception {
                    // 将数据转换成二元组，方便计算
                    return Tuple2.of(value.user, 1L);
                }
            })
            .keyBy(r -> r.f0)
            // 设置滚动事件时间窗口
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                   Tuple2<String, Long> value2) throws Exception {
                    // 定义累加规则，窗口闭合时，向下游发送累加结果
                    return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                }
            })
            .print();

        env.execute();
    }

    /**** 将5秒窗口内的数据（1秒一条共5条）进行按用户维度统计计数 *****/
    //------in---Event{user='Alice', url='./home', ts=2022-05-11 20:12:45.374}
    //------in---Event{user='Cary', url='./cart', ts=2022-05-11 20:12:46.378}
    //------in---Event{user='Mary', url='./prod?id=1', ts=2022-05-11 20:12:47.388}
    //------in---Event{user='Bob', url='./home', ts=2022-05-11 20:12:48.393}
    //------in---Event{user='Mary', url='./fav', ts=2022-05-11 20:12:49.397}
    //(Alice,1)
    //(Cary,1)
    //(Bob,1)
    //(Mary,2)
    //------in---Event{user='Alice', url='./home', ts=2022-05-11 20:12:50.407}
    //------in---Event{user='Cary', url='./prod?id=2', ts=2022-05-11 20:12:51.41}
    //------in---Event{user='Bob', url='./home', ts=2022-05-11 20:12:52.412}
    //------in---Event{user='Cary', url='./prod?id=2', ts=2022-05-11 20:12:53.422}
    //------in---Event{user='Alice', url='./fav', ts=2022-05-11 20:12:54.434}
    //(Alice,2)
    //(Cary,2)
    //(Bob,1)
    //------in---Event{user='Bob', url='./prod?id=2', ts=2022-05-11 20:12:55.438}
    //------in---Event{user='Bob', url='./fav', ts=2022-05-11 20:12:56.448}
    //------in---Event{user='Bob', url='./prod?id=1', ts=2022-05-11 20:12:57.449}
    //------in---Event{user='Alice', url='./home', ts=2022-05-11 20:12:58.456}
    //------in---Event{user='Alice', url='./prod?id=2', ts=2022-05-11 20:12:59.465}
    //(Bob,3)
    //(Alice,2)
}


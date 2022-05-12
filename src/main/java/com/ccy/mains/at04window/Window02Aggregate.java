package com.ccy.mains.at04window;

import com.ccy.entity.Event;
import com.ccy.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;

public class Window02Aggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
            );

        // 所有数据设置相同的key，发送到同一个分区统计PV和UV，再相除
        stream.keyBy(data -> true)
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
            .aggregate(new AvgPv())
            .print();

        env.execute();
    }

    public static class AvgPv implements AggregateFunction<Event,
            Tuple2<HashSet<String>, Long>, Double> {
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            // 创建累加器
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(Event value,
                                                 Tuple2<HashSet<String>, Long> accumulator) {
            // 属于本窗口的数据来一条累加一次，并返回累加器
            accumulator.f0.add(value.user);
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            // 窗口闭合时，增量聚合结束，将计算结果发送到下游
            return (double) accumulator.f1 / accumulator.f0.size();
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a,
                                                   Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }

    //PV 统计的是所有的点击量；
    //而对用户 id 进行去重之后，得到的就是 UV。
    //用 PV/UV 这个比值，来表示“人均重复访问量”，也就是平均每个用户会访问多少次页面，这在一定程度上代表了用户的粘度。
    //
    //------in---Event{user='Alice', url='./cart', ts=2022-05-11 20:45:56.127}
    //------in---Event{user='Alice', url='./prod?id=2', ts=2022-05-11 20:45:57.148}
    //PV=2 UV=1（Alice） PV/UV=
    //2.0
    //------in---Event{user='Mary', url='./prod?id=1', ts=2022-05-11 20:45:58.15}
    //------in---Event{user='Alice', url='./prod?id=1', ts=2022-05-11 20:45:59.159}
    //PV=4 UV=2（Alice/Mary） PV/UV=
    //2.0
    //------in---Event{user='Alice', url='./fav', ts=2022-05-11 20:46:00.167}
    //------in---Event{user='Bob', url='./prod?id=2', ts=2022-05-11 20:46:01.169}
    //PV=6 UV=3（Alice/Mary/Bob） PV/UV=
    //2.0
    //------in---Event{user='Mary', url='./fav', ts=2022-05-11 20:46:02.179}
    //------in---Event{user='Alice', url='./home', ts=2022-05-11 20:46:03.186}
    //PV=8 UV=3（Alice/Mary/Bob） PV/UV=
    //2.6666666666666665
    //------in---Event{user='Cary', url='./prod?id=1', ts=2022-05-11 20:46:04.193}
    //------in---Event{user='Alice', url='./prod?id=2', ts=2022-05-11 20:46:05.202}
    //PV=10 UV=4（Alice/Mary/Bob/Cary） PV/UV=
    //2.5
}


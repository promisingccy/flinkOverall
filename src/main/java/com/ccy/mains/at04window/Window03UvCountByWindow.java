package com.ccy.mains.at04window;

import com.ccy.entity.Event;
import com.ccy.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class Window03UvCountByWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(
                //乱序流 WatermarkStrategy.forBoundedOutOfOrderness()
                //有序流 WatermarkStrategy.forMonotonousTimestamps()
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    }
            ));

        // 将数据全部发往同一分区，按窗口统计UV
        stream.keyBy(data -> true)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new UvCountByWindow())
            .print();

        env.execute();
    }

    // 自定义窗口处理函数
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            // 遍历所有数据，放到Set里去重
            for (Event event: elements){
                userSet.add(event.user);
            }
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("窗口: " + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " 的独立访客数量是：" + userSet.size());
        }
    }

    //{user='Cary', url='./cart', ts=2022-05-12 09:49:11.831}
    //{user='Alice', url='./prod?id=1', ts=2022-05-12 09:49:12.831}
    //{user='Bob', url='./fav', ts=2022-05-12 09:49:13.831}
    //{user='Bob', url='./fav', ts=2022-05-12 09:49:14.841}
    //{user='Alice', url='./home', ts=2022-05-12 09:49:15.841}
    //{user='Cary', url='./prod?id=1', ts=2022-05-12 09:49:16.851}
    //{user='Bob', url='./prod?id=1', ts=2022-05-12 09:49:17.853}
    //{user='Cary', url='./home', ts=2022-05-12 09:49:18.853}
    //{user='Alice', url='./prod?id=1', ts=2022-05-12 09:49:19.863}
    //{user='Alice', url='./prod?id=2', ts=2022-05-12 09:49:20.873}
    //窗口: 2022-05-12 09:49:10.0 ~ 2022-05-12 09:49:20.0 的独立访客数量是：3

    //{user='Bob', url='./prod?id=1', ts=2022-05-12 09:49:21.873}
    //{user='Bob', url='./fav', ts=2022-05-12 09:49:22.883}
    //{user='Cary', url='./cart', ts=2022-05-12 09:49:23.883}
    //{user='Mary', url='./home', ts=2022-05-12 09:49:24.884}
    //{user='Alice', url='./prod?id=1', ts=2022-05-12 09:49:25.893}
    //{user='Cary', url='./prod?id=1', ts=2022-05-12 09:49:26.896}
    //{user='Mary', url='./home', ts=2022-05-12 09:49:27.905}
    //{user='Mary', url='./prod?id=2', ts=2022-05-12 09:49:28.905}
    //{user='Alice', url='./fav', ts=2022-05-12 09:49:29.915}
    //{user='Mary', url='./cart', ts=2022-05-12 09:49:30.925}
    //窗口: 2022-05-12 09:49:20.0 ~ 2022-05-12 09:49:30.0 的独立访客数量是：4
}


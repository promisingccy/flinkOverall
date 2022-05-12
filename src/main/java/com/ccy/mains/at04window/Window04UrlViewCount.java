package com.ccy.mains.at04window;

import com.ccy.entity.Event;
import com.ccy.entity.UrlViewCount;
import com.ccy.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class Window04UrlViewCount {
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
                }));

        // 需要按照url分组，开滑动窗口统计
        stream.keyBy(data -> data.url)
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            // 同时传入增量聚合函数和全窗口函数
            .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
            .print();

        env.execute();
    }

    // 自定义增量聚合函数，来一条数据就加一
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }
        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 自定义窗口处理函数，只需要包装窗口信息
    public static class UrlViewCountResult
            extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url,
                            Context context,
                            Iterable<Long> elements,
                            Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            // 迭代器中只有一个元素，就是增量聚合函数的计算结果
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    //{user='Bob', url='./prod?id=1', ts=2022-05-12 14:08:16.227}
    //{user='Cary', url='./prod?id=1', ts=2022-05-12 14:08:17.227}
    //{user='Bob', url='./fav', ts=2022-05-12 14:08:18.227}
    //{user='Mary', url='./cart', ts=2022-05-12 14:08:19.23}
    ////窗口为 2022-05-12 14:08:10.0 - 2022-05-12 14:08:20.0 中的top 访问url
    //UrlViewCount{url='./prod?id=1', count=2, windowStart=, windowEnd=}
    //UrlViewCount{url='./fav', count=1, windowStart=, windowEnd=}
    //UrlViewCount{url='./cart', count=1, windowStart=, windowEnd=}
    //
    //{user='Mary', url='./fav', ts=2022-05-12 14:08:20.237}
    //{user='Mary', url='./home', ts=2022-05-12 14:08:21.237}
    //{user='Cary', url='./cart', ts=2022-05-12 14:08:22.237}
    //{user='Mary', url='./fav', ts=2022-05-12 14:08:23.246}
    //{user='Bob', url='./fav', ts=2022-05-12 14:08:24.246}
    ////窗口为 2022-05-12 14:08:15.0 - 2022-05-12 14:08:25.0 中的top 访问url
    //UrlViewCount{url='./cart', count=2, windowStart=, windowEnd=}
    //UrlViewCount{url='./prod?id=1', count=2, windowStart=, windowEnd=}
    //UrlViewCount{url='./fav', count=4, windowStart=, windowEnd=}
    //UrlViewCount{url='./home', count=1, windowStart=, windowEnd=}
    //
    //{user='Mary', url='./cart', ts=2022-05-12 14:08:25.256}
    //{user='Mary', url='./prod?id=1', ts=2022-05-12 14:08:26.256}
    //{user='Mary', url='./home', ts=2022-05-12 14:08:27.256}
    //{user='Cary', url='./prod?id=2', ts=2022-05-12 14:08:28.257}
    //{user='Mary', url='./home', ts=2022-05-12 14:08:29.257}
    ////窗口为 2022-05-12 14:08:20.0 - 2022-05-12 14:08:30.0 中的top 访问url
    //UrlViewCount{url='./home', count=3, windowStart=, windowEnd=}
    //UrlViewCount{url='./fav', count=3, windowStart=, windowEnd=}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=, windowEnd=}
    //UrlViewCount{url='./cart', count=2, windowStart=, windowEnd=}
    //UrlViewCount{url='./prod?id=1', count=1, windowStart=, windowEnd=}
    //
    //{user='Mary', url='./cart', ts=2022-05-12 14:08:30.258}
    //{user='Bob', url='./cart', ts=2022-05-12 14:08:31.258}
    //{user='Alice', url='./prod?id=1', ts=2022-05-12 14:08:32.258}
    //{user='Bob', url='./prod?id=1', ts=2022-05-12 14:08:33.268}
    //{user='Mary', url='./fav', ts=2022-05-12 14:08:34.268}
    ////窗口为 2022-05-12 14:08:25.0 - 2022-05-12 14:08:35.0 中的top 访问url
    //UrlViewCount{url='./cart', count=3, windowStart=, windowEnd=}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=, windowEnd=}
    //UrlViewCount{url='./fav', count=1, windowStart=, windowEnd=}
    //UrlViewCount{url='./prod?id=1', count=3, windowStart=, windowEnd=}
    //UrlViewCount{url='./home', count=2, windowStart=, windowEnd=}
}

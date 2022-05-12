package com.ccy.mains.at04window;

import com.ccy.entity.Event;
import com.ccy.entity.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


public class Window07ProcessLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取socket文本流
        SingleOutputStreamOperator<Event> stream =
            env.socketTextStream("10.0.81.118", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(" ");
                        return new Event(
                                fields[0].trim(),
                                fields[1].trim(),
                                Long.valueOf(fields[2].trim()));
                    }
                })
                // 方式一：设置watermark延迟时间，2秒钟
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 定义侧输出流标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            // 方式二：允许窗口处理迟到数据，设置1分钟的等待时间
            .allowedLateness(Time.minutes(1))
            // 方式三：将最后的迟到数据输出到侧输出流
            .sideOutputLateData(outputTag)
            .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        result.print("result");
        result.getSideOutput(outputTag).print("late");

        // 为方便观察，可以将原始数据也输出
        stream.print("input");

        env.execute();
    }

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
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    // 方式一：设置watermark延迟时间，2秒钟
    // 方式二：10秒滚动窗口，允许窗口处理迟到数据，设置1分钟的等待时间
    // 方式三：将最后的迟到数据输出到侧输出流
    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:01.0}
    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:02.0}
    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:10.0}
    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:09.0}
    //input> Event{user='Alice,', url='./cart,', ts=1970-01-01 08:00:12.0}
    //input> Event{user='Alice,', url='./prod?id=100,', ts=1970-01-01 08:00:15.0}
    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:09.0}
    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:08.0}
    //input> Event{user='Alice,', url='./prod?id=200,', ts=1970-01-01 08:01:10.0}
    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:08.0}
    //input> Event{user='Alice,', url='./prod?id=300,', ts=1970-01-01 08:01:12.0}
    //----> 10秒窗口 [1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0)
    //result> UrlViewCount{url='./home,', count=6, windowStart=1970-01-01 08:00:00.0, windowEnd=1970-01-01 08:00:10.0}
    //result> UrlViewCount{url='./cart,', count=1, windowStart=1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0}
    //result> UrlViewCount{url='./home,', count=1, windowStart=1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0}
    //result> UrlViewCount{url='./prod?id=100,', count=1, windowStart=1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0}

    //input> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:08.0}
    //----> [1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0) 已经关闭，之后算迟到数据，进入测输出流
    //late> Event{user='Alice,', url='./home,', ts=1970-01-01 08:00:08.0}
}


package com.ccy.mains.at04window;

import com.ccy.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Window05Watermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将数据源改为socket文本流，并转换成Event类型
        env.socketTextStream("10.0.81.118", 7777)
            .map(new MapFunction<String, Event>() {
                @Override
                public Event map(String value) throws Exception {
                    String[] fields = value.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                }
            })
            // 插入水位线的逻辑
            .assignTimestampsAndWatermarks(
                // 针对乱序流插入水位线，延迟时间设置为5s
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        // 抽取时间戳的逻辑
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    })
            )
            // 根据user分组，开窗统计
            .keyBy(data -> data.user)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new WatermarkTestResult())
            .print();

        env.execute();
    }

    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long currentWatermark = context.currentWatermark();
            Long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
        }
    }

    //10秒滚动窗口 水位线延迟时间为5s
    //Alice, ./home, 1000
    //Alice, ./cart, 2000
    //Alice, ./prod?id=100, 10000
    //Alice, ./prod?id=200, 8000
    //Alice, ./prod?id=300, 15000
    //--->这时流中会周期性地（默认 200毫秒）插入一个时间戳为
    // 15000L - 5 * 1000L - 1L = 9999 毫秒的水位线，
    // 已经到达了窗口[0,10000)的结束时间，所以会触发窗口的闭合计算。
    //---> 窗口0 ~ 10000中共有3个元素，窗口闭合计算时，水位线处于：9999

    //Alice, ./prod?id=200, 9000
    //--->这时将不会有任何结果；因为这是一条迟到数据，它所属于的窗口已经触发计算然后销毁了（窗口默认被销毁），
    // 所以无法再进入到窗口中，自然也就无法更新计算结果了。
    // 窗口中的迟到数据默认会被丢弃，这会导致计算结果不够准确。
}




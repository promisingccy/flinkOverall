package com.ccy.mains.at04window;

import com.ccy.entity.Event;
import com.ccy.entity.UrlViewCount;
import com.ccy.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Window06Trigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(
                //乱序流 WatermarkStrategy.forBoundedOutOfOrderness()
                //有序流 WatermarkStrategy.forMonotonousTimestamps()
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event event, long l) {
                            return event.timestamp;
                        }
                    })
            )
            .keyBy(r -> r.url)
            //滚动窗口 10秒
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .trigger(new MyTrigger())
            .process(new WindowResult())
            .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Event, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s,
                            Context context,
                            Iterable<Event> iterable,
                            Collector<UrlViewCount> collector) throws Exception {
            collector.collect(
                new UrlViewCount(
                    s,
                    // 获取迭代器中的元素个数
                    iterable.spliterator().getExactSizeIfKnown(),
                    context.window().getStart(),
                    context.window().getEnd()
                )
            );
        }
    }

    public static class MyTrigger extends Trigger<Event, TimeWindow> {
        @Override
        //窗口中每到来一个元素，都会调用这个方法。
        public TriggerResult onElement(Event event, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN)
            );
            if (isFirstEvent.value() == null) {
                for (long i = timeWindow.getStart(); i < timeWindow.getEnd(); i = i + 1000L) {
                    triggerContext.registerEventTimeTimer(i);
                }
                isFirstEvent.update(true);
            }
            //⚫ CONTINUE（继续）：什么都不做
            //⚫ FIRE（触发）：触发计算，输出结果
            //⚫ PURGE（清除）：清空窗口中的所有数据，销毁窗口
            //⚫ FIRE_AND_PURGE（触发并清除）：触发计算输出结果，并清除窗口
            return TriggerResult.CONTINUE;
        }

        @Override
        //当注册的事件时间定时器触发时，将调用这个方法。
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        //当注册的处理时间定时器触发时，将调用这个方法。
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        //当窗口关闭销毁时，调用这个方法。一般用来清除自定义的状态。
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN)
            );
            isFirstEvent.clear();
        }
    }

    ////计算了每个 url 在 10 秒滚动窗口的 pv 指标，然后设置了触发器，每隔 1 秒钟触发一次窗口的计算
    ////PV 统计的是所有的点击量；
    //--->{user='Cary', url='./fav', ts=2022-05-12 15:07:50.677}
    //UrlViewCount{url='./fav', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //
    //--->{user='Mary', url='./cart', ts=2022-05-12 15:07:51.688}
    //UrlViewCount{url='./cart', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./fav', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./cart', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //
    //--->{user='Cary', url='./prod?id=1', ts=2022-05-12 15:07:52.689}
    //UrlViewCount{url='./prod?id=1', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=1', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./fav', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=1', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./cart', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //
    //--->{user='Alice', url='./prod?id=1', ts=2022-05-12 15:07:53.69}
    //UrlViewCount{url='./fav', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=1', count=2, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./cart', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //
    //--->{user='Bob', url='./prod?id=2', ts=2022-05-12 15:07:54.7}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./fav', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=1', count=2, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./cart', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //
    //--->{user='Cary', url='./home', ts=2022-05-12 15:07:55.709}
    //UrlViewCount{url='./home', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./home', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./home', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./home', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./home', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./fav', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./cart', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=2', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./prod?id=1', count=2, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
    //UrlViewCount{url='./home', count=1, windowStart=2022-05-12 15:07:50.0, windowEnd=2022-05-12 15:08:00.0}
}


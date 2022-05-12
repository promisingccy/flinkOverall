package com.ccy.mains.at05processFunction;

import com.ccy.entity.Event;
import com.ccy.source.ClickSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessFunction02ProcessingTimeTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 处理时间语义，不需要分配时间戳和watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        // 要用定时器，必须基于KeyedStream

        stream
            // 将所有数据的 key 都指定为了 true，其实就是所有数据拥有相同的 key，会分配到同一个分区。
            .keyBy(data -> true)
            .process(new KeyedProcessFunction<Boolean, Event, String>() {
                @Override
                public void processElement(Event value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    Long currTs = ctx.timerService().currentProcessingTime();
                    out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                    // 注册一个5秒后的定时器
                    ctx.timerService().registerProcessingTimeTimer(currTs + 5 * 1000L);
                }

                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                }
            })
            .print();

        env.execute();

        //--->{user='Alice', url='./cart', ts=2022-05-12 17:27:38.815}
        //数据到达，到达时间：2022-05-12 17:27:38.855

        //--->{user='Mary', url='./prod?id=1', ts=2022-05-12 17:27:39.815}
        //数据到达，到达时间：2022-05-12 17:27:39.885

        //--->{user='Mary', url='./prod?id=1', ts=2022-05-12 17:27:40.815}
        //数据到达，到达时间：2022-05-12 17:27:40.855

        //--->{user='Cary', url='./prod?id=1', ts=2022-05-12 17:27:41.816}
        //数据到达，到达时间：2022-05-12 17:27:41.886

        //--->{user='Cary', url='./cart', ts=2022-05-12 17:27:42.816}
        //数据到达，到达时间：2022-05-12 17:27:42.836

        //--->{user='Cary', url='./fav', ts=2022-05-12 17:27:43.816}
        //@5秒到了@定时器触发，触发时间：2022-05-12 17:27:43.855
        //数据到达，到达时间：2022-05-12 17:27:43.896

        //--->{user='Alice', url='./home', ts=2022-05-12 17:27:44.826}
        //@5秒到了@定时器触发，触发时间：2022-05-12 17:27:44.885
        //数据到达，到达时间：2022-05-12 17:27:44.926

        //--->{user='Alice', url='./home', ts=2022-05-12 17:27:45.826}
        //@5秒到了@定时器触发，触发时间：2022-05-12 17:27:45.855
        //数据到达，到达时间：2022-05-12 17:27:45.906

        //--->{user='Alice', url='./fav', ts=2022-05-12 17:27:46.835}
        //数据到达，到达时间：2022-05-12 17:27:46.845
        //@5秒到了@定时器触发，触发时间：2022-05-12 17:27:46.886

        //--->{user='Mary', url='./prod?id=2', ts=2022-05-12 17:27:47.844}
        //@5秒到了@定时器触发，触发时间：2022-05-12 17:27:47.836
        //数据到达，到达时间：2022-05-12 17:27:47.895
    }
}


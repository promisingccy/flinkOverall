package com.ccy.mains.at05processFunction;

import com.ccy.entity.Event;
import com.ccy.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunction01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event event, long l) {
                            return event.timestamp;
                        }
                    })
            )
            .process(new ProcessFunction<Event, String>() {
                @Override
                public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if (value.user.equals("Mary")) {
                    out.collect(value.user);
                } else if (value.user.equals("Bob")) {
                    out.collect(value.user);
                    out.collect(value.user);
                }
                System.out.println(ctx.timerService().currentWatermark());
                }
            })
            .print();

        env.execute();

        //{user='Mary', url='./prod?id=2', ts=2022-05-12 16:51:28.593}
        //Mary
        //-9223372036854775808

        //{user='Mary', url='./home', ts=2022-05-12 16:51:29.594}
        //Mary
        //1652345488592

        //{user='Bob', url='./prod?id=1', ts=2022-05-12 16:51:30.594}
        //Bob
        //Bob
        //1652345489593

        //{user='Alice', url='./cart', ts=2022-05-12 16:51:31.594}
        //1652345490593

        //{user='Mary', url='./home', ts=2022-05-12 16:51:32.604}
        //Mary
        //1652345491593

        //{user='Cary', url='./cart', ts=2022-05-12 16:51:33.604}
        //1652345492603

        //{user='Bob', url='./prod?id=2', ts=2022-05-12 16:51:34.604}
        //Bob
        //Bob
        //1652345493603
    }
}


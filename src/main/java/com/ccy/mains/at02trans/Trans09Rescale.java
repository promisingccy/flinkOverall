package com.ccy.mains.at02trans;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Trans09Rescale {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 8; i++) {
                    if((i+1) % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                        sourceContext.collect(i+1);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });

        stream.setParallelism(2).rebalance().print().setParallelism(4);

        //-------------- rescale() 局部重排
        //奇数的线程号 3434  偶数的线程号 1212
            //3> 1
        //1> 2
            //4> 3
        //2> 4
            //3> 5
        //1> 6
            //4> 7
        //2> 8

        //-------------- rebalance() 全量重排
        //奇数的线程号 3412  偶数的线程号 4123
            //3> 1
        //4> 2
            //4> 3
        //1> 4
            //1> 5
        //2> 6
            //2> 7
        //3> 8
        env.execute();
    }
}

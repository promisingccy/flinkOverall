package com.ccy.mains.at02trans;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Trans10CustomPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //lambda写法
        env.fromElements(1,2,3,4,5,6,7,8)
                .partitionCustom(
                        (Partitioner<Integer>) (key, numPartitions) -> (key+1)%2,
                        (KeySelector<Integer, Integer>) value -> value
                ).print()
                .setParallelism(2);


        env.fromElements(1,2,3,4,5,6,7,8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return (key+1)%2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).print()
                .setParallelism(2);

        //key%2
        //1> 2
        //1> 4
        //1> 6
        //1> 8

        //2> 1
        //2> 3
        //2> 5
        //2> 7

        //(key+1)%2
        //2> 2
        //2> 4
        //2> 6
        //2> 8

        //1> 1
        //1> 3
        //1> 5
        //1> 7

        env.execute();
    }
}

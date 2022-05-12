package com.ccy.mains.at02trans;

import com.ccy.entity.Event;
import com.ccy.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Trans06Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里的使用了之前自定义数据源小节中的ClickSource()
        env.addSource(new ClickSource())
                // 将Event数据类型转换成元组类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event e) throws Exception {
                        return Tuple2.of(e.user, 1L);
                    }
                })

                /************ 统计累加每个用户出现的次数 start **************/
                .keyBy(r -> r.f0) // 使用用户名来进行分流
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 每到一条数据，用户pv的统计值加1
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                /************ 统计累加每个用户出现的次数 end **************/


                /************ 只输出出现次数最高的用户 start **************/
                .keyBy(r -> true) // 为每一条数据分配同一个key，将聚合结果发送到一条流中去
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                /************ 只输出出现次数最高的用户 end **************/
                .print();

        env.execute();

/**  只看次数，用户名变化
 ------in---Event{user='Mary', url='./prod?id=1', timestamp=2022-04-28 10:47:29.593}
 (Mary,1)
 ------in---Event{user='Cary', url='./fav', timestamp=2022-04-28 10:47:30.607}
 (Cary,1)
 ------in---Event{user='Mary', url='./prod?id=1', timestamp=2022-04-28 10:47:31.609}
 (Mary,2)
 ------in---Event{user='Alice', url='./cart', timestamp=2022-04-28 10:47:32.61}
 (Mary,2)
 ------in---Event{user='Alice', url='./home', timestamp=2022-04-28 10:47:33.611}
 (Alice,2)
 ------in---Event{user='Alice', url='./home', timestamp=2022-04-28 10:47:34.612}
 (Alice,3)
 ------in---Event{user='Mary', url='./prod?id=2', timestamp=2022-04-28 10:47:35.616}
 (Mary,3)
 ------in---Event{user='Mary', url='./prod?id=1', timestamp=2022-04-28 10:47:36.617}
 (Mary,4)
 ------in---Event{user='Bob', url='./home', timestamp=2022-04-28 10:47:37.63}
 (Mary,4)
 ------in---Event{user='Alice', url='./home', timestamp=2022-04-28 10:47:38.633}
 (Alice,4)
 ------in---Event{user='Alice', url='./cart', timestamp=2022-04-28 10:47:39.634}
 (Alice,5)
 ------in---Event{user='Cary', url='./cart', timestamp=2022-04-28 10:47:40.646}
 (Alice,5)
 ------in---Event{user='Bob', url='./cart', timestamp=2022-04-28 10:47:41.648}
 (Alice,5)
 ------in---Event{user='Cary', url='./fav', timestamp=2022-04-28 10:47:42.65}
 (Alice,5)
 ------in---Event{user='Cary', url='./prod?id=1', timestamp=2022-04-28 10:47:43.652}
 (Alice,5)
 ------in---Event{user='Mary', url='./home', timestamp=2022-04-28 10:47:44.656}
 (Mary,5)
 ------in---Event{user='Alice', url='./fav', timestamp=2022-04-28 10:47:45.671}
 (Alice,6)
 ------in---Event{user='Cary', url='./prod?id=2', timestamp=2022-04-28 10:47:46.684}
 (Alice,6)
 ------in---Event{user='Alice', url='./home', timestamp=2022-04-28 10:47:47.685}
 (Alice,7)
 ------in---Event{user='Mary', url='./home', timestamp=2022-04-28 10:47:48.699}
 (Alice,7)
 ------in---Event{user='Cary', url='./home', timestamp=2022-04-28 10:47:49.702}
 (Alice,7)
 ------in---Event{user='Bob', url='./cart', timestamp=2022-04-28 10:47:50.716}
 (Alice,7)
 */
    }
}


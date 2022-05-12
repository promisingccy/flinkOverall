package com.ccy.source;

import com.ccy.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickSource.class);
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();    // 在指定的数据集中随机选取数据
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            Event event = new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            );
            LOG.info("------in---{}", event.toString());
            ctx.collect(event);
            // 隔1秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }

}

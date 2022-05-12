package com.ccy.sink;

import cn.hutool.core.date.SystemClock;
import com.alibaba.fastjson.JSONObject;
import com.ccy.config.Configs;
import com.ccy.core.RestClientFactoryImpl;
import com.ccy.core.ThreadLocalDateUtils;
import com.ccy.exception.EsActionRequestFailureHandler;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class EsSink {
    private static final Logger logger = LoggerFactory.getLogger(EsSink.class);

    public static ElasticsearchSink.Builder<JSONObject> sinkEs(String index) {
        //es 域名端口
        List<HttpHost> httpHosts = new ArrayList<>();
        String[] hosts = Configs.ES_HOSTS.split(",");
        for(String host:hosts){
            String[] split = host.split(":");
            httpHosts.add(new HttpHost(split[0], Integer.parseInt(split[1]), Configs.ES_SCHEME));
        }

        ElasticsearchSink.Builder<JSONObject> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
            new ElasticsearchSinkFunction<JSONObject>() {
                public IndexRequest createIndexRequest(JSONObject element) {
                    Long ts = element.getLong("ts");
                    String[] ts_date = null;
                    String ts_day = null;
                    try {
                        ts_date = ThreadLocalDateUtils.format_ts(ts).split(" ");
                    } catch (ParseException e) {
                        logger.info("时间格式话失败：" + ts);
                    }
                    if (ts_date == null || ts > SystemClock.now()+3600000) {
                        ts_day = "19700101";
                    } else {
                        ts_day = ts_date[0];
                    }
                    return Requests.indexRequest().index(index + "-" + ts_day).source(element);
                }

                @Override
                public void process(JSONObject element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            }
        );
        //批量写入时的最大写入条数
        esSinkBuilder.setBulkFlushMaxActions(Configs.ES_MAXCOUNT);
        //批量写入时的最大数据量
        esSinkBuilder.setBulkFlushMaxSizeMb(Configs.ES_MAXSIZE);
        //批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置
        if(-1 != Configs.ES_FLUSH_INTERVAL){
            esSinkBuilder.setBulkFlushInterval(Configs.ES_FLUSH_INTERVAL);
        }
        //进行重试的时间间隔
        esSinkBuilder.setBulkFlushBackoffDelay(Configs.ES_FLUSH_BACKOFFDELAY);
        //失败重试的次数
        esSinkBuilder.setBulkFlushBackoffRetries(Configs.ES_FLUSHRETARY);
        // 重试策略，有两种：EXPONENTIAL 指数型（表示多次重试之间的时间间隔按照指数方式进行增长）、CONSTANT 常数型（表示多次重试之间的时间间隔为固定常数）
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
        //Es 设置用户名，密码，证书
        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl(Configs.ES_USERNAME, Configs.ES_PASSWORD, Configs.ES_CERTIFICATE));
        //设置Es 失败策略
        esSinkBuilder.setFailureHandler(new EsActionRequestFailureHandler());

        return esSinkBuilder;
    }
}

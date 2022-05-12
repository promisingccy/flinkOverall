package com.ccy.exception;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;

//自定义Es失败补偿策略
public class EsActionRequestFailureHandler implements ActionRequestFailureHandler {
    private static final Logger logger = LoggerFactory.getLogger(EsActionRequestFailureHandler.class);

    @Override
    public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
        if(failure instanceof ElasticsearchException && failure.getMessage().indexOf("type=circuit_breaking_exception") > 0){
            //data too large
            indexer.add(new ActionRequest[]{action});
            logger.error("circuit_breaking_exception" + failure.getMessage());
        } else if (failure instanceof ElasticsearchException && failure.getMessage().indexOf("type=mapper_parsing_exception") > 0) {
            logger.error("es mapping parsing Exception：" + action.toString() + "错误写出信息：" + failure.getMessage());
        } else if (failure instanceof ElasticsearchException && failure.getMessage().indexOf("type=index_not_found_exception") > 0) {
            //该错误数据结构没有异常，index 也存在，但是Es 会偶尔出现这样的问题，是es 一个bug
            indexer.add(new ActionRequest[]{action});
            logger.error("es index not found exception：" + action.toString() + "错误写出信息：" + failure.getMessage());
        } else if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
            //es 队列满了（reject 异常，放回队列）
            indexer.add(new ActionRequest[]{action});
            logger.error(failure.getMessage());
        } else if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
            //ES超时异常(timeout异常)，Es 会触发自身的重试机制
            logger.error(failure.getMessage());
            return;
        } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
            // ES语法异常，丢弃数据，记录日志
            logger.error("es 语法解析异常：" + action.toString() + "错误写出信息：" + failure.getMessage());
        } else {
            //其他异常情况
            logger.error("es 其他异常写入失败：" + action.toString() + "错误写出信息：" + failure.getMessage());
        }
    }
}

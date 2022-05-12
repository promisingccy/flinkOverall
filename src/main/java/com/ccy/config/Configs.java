package com.ccy.config;

public class Configs {
    //constant es
    public static final String ES_CERTIFICATE = "D:/es/xx.crt";
    public static final String ES_PASSWORD = "xx";
    public static final String ES_USERNAME = "xx";
    public static final String ES_HOSTS = "xx-master:9200,xx-secondary:9200,xx-slave:9200";
    public static final String ES_PORT = "9200";
    public static final String ES_SCHEME = "https";
    //获取Es 最大写入条数
    public static final int ES_MAXCOUNT = 100000;
    public static final int ES_MAXSIZE = 15;
    public static final int ES_FLUSHRETARY = 3;
    public static final int ES_FLUSH_INTERVAL = 10000;
    public static final int ES_FLUSH_BACKOFFDELAY = 10;

}

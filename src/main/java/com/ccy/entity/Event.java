package com.ccy.entity;

import com.alibaba.fastjson.JSONObject;

import java.sql.Timestamp;

public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public JSONObject toJson(){
        JSONObject res = new JSONObject();
        res.put("user", user);
        res.put("url", url);
        res.put("ts", timestamp);
        return res;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + new Timestamp(timestamp) +
                '}';
    }
}

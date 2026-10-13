package com.rookie.constants;

public class Constants {
    public static final String HOST = "192.168.233.128";
    public static final int PORT = 5672;
    public static final String USER_NAME = "admin";
    public static final String PASSWORD = "admin";
    public static final String VIRTUAL_HOST = "study";

    //工作队列模式
    public static final String WORK_QUEUE = "work.queue";

    //发布订阅模式
    //fanout
    public static final String FANOUT_EXCHANGE = "fanout.exchange";
    public static final String FANOUT_QUEUE1 = "fanout.queue1";
    public static final String FANOUT_QUEUE2 = "fanout.queue2";

    //direct
    public static final String DIRECT_EXCHANGE = "direct.exchange";
    public static final String DIRECT_QUEUE1 = "direct.queue1";
    public static final String DIRECT_QUEUE2 = "direct.queue2";

    //topic
    public static final String TOPIC_EXCHANGE = "topic.exchange";
    public static final String TOPIC_QUEUE1 = "topic.queue1";
    public static final String TOPIC_QUEUE2 = "topic.queue2";

    //RPC模式
    public static final String RPC_REQUEST_QUEUE = "rpc.request.queue";
    public static final String RPC_RESPONSE_QUEUE = "rpc.response.queue";

    //确认模式
    public static final String PUBLISHER_CONFIRMS_QUEUE1 = "publisher.confirms.queue1";
    public static final String PUBLISHER_CONFIRMS_QUEUE2 = "publisher.confirms.queue2";
    public static final String PUBLISHER_CONFIRMS_QUEUE3 = "publisher.confirms.queue3";
}

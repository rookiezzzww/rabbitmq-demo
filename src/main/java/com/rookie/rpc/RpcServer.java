package com.rookie.rpc;

import com.rabbitmq.client.*;
import com.rookie.constants.Constants;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RpcServer {
    public static void main(String[] args) throws IOException, TimeoutException {
        //1. 建立连接
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(Constants.HOST);
        connectionFactory.setPort(Constants.PORT); //需要提前开放端口号
        connectionFactory.setUsername(Constants.USER_NAME);//账号
        connectionFactory.setPassword(Constants.PASSWORD);  //密码
        connectionFactory.setVirtualHost(Constants.VIRTUAL_HOST); //虚拟主机
        Connection connection = connectionFactory.newConnection();
        //2. 开启信道
        Channel channel = connection.createChannel();
        //3. 声明队列
//        channel.queueDeclare(Constants.RPC_REQUEST_QUEUE, false, false, false, null);
        //4.消费消息
        channel.basicQos(1);//表示每个消费者最多同时持有 1 条未确认的消息，ack 之后才能拿新的
        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, com.rabbitmq.client.Envelope envelope, com.rabbitmq.client.AMQP.BasicProperties properties, byte[] body) throws IOException {
                String request = new String(body, "UTF-8");
                System.out.println("接收到client请求:"+ request);
                String response = "针对request:"+ request +", 响应成功";
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .correlationId(properties.getCorrelationId())
                        .build();
                channel.basicPublish("", properties.getReplyTo(), props, response.getBytes("UTF-8"));//需要手动确认
                channel.basicAck(envelope.getDeliveryTag(), false);//需要手动确认
            }
        };
        channel.basicConsume(Constants.RPC_REQUEST_QUEUE, false, consumer);
    }
}

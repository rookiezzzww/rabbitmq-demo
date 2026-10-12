package com.rookie.publish_subscribe.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rookie.constants.Constants;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {
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
        //3.声明交换机
        channel.exchangeDeclare(Constants.FANOUT_EXCHANGE, BuiltinExchangeType.FANOUT, true);
        //4. 声明队列   使用内置的交换机
        channel.queueDeclare(Constants.FANOUT_QUEUE1, true, false, false, null);
        channel.queueDeclare(Constants.FANOUT_QUEUE2, true, false, false, null);
        //4.交换机绑定队列
        channel.queueBind(Constants.FANOUT_QUEUE1, Constants.FANOUT_EXCHANGE, "");
        channel.queueBind(Constants.FANOUT_QUEUE2, Constants.FANOUT_EXCHANGE, "");
        //5. 发送消息
        String msg = "hello fanout exchange....";
        channel.basicPublish(Constants.FANOUT_EXCHANGE, "", null, msg.getBytes());
        System.out.println("消息发送成功~");
        //6. 资源释放
        connection.close();
    }
}

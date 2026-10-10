package com.rookie.work;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rookie.constants.Constants;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProducerDemo {
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
        //3. 声明队列   使用内置的交换机
        //如果队列不存在, 则创建, 如果队列存在, 则不创建
        channel.queueDeclare(Constants.WORK_QUEUE, true, false, false, null);
        //4. 发送消息
        for (int i = 0; i < 10; i++) {
            String msg = "hello work queue...."+i;
            channel.basicPublish("",Constants.WORK_QUEUE, null, msg.getBytes());
        }

        System.out.println("消息发送成功~");
        //6. 资源释放
        channel.close();
        connection.close();
    }
}
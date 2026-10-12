package com.rookie.publish_subscribe.direct;

import com.rabbitmq.client.*;
import com.rookie.constants.Constants;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer2 {
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
        channel.queueDeclare(Constants.DIRECT_QUEUE2, true, false, false, null);
        //4. 消费消息
        DefaultConsumer consumer = new DefaultConsumer(channel){
            //从队列中收到消息, 就会执行的方法
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("接收到消息:"+ new String(body));
            }
        };
        channel.basicConsume(Constants.DIRECT_QUEUE2, true, consumer);

//        //5. 资源释放
//        connection.close();
    }
}

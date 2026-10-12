package com.rookie.work;

import com.rabbitmq.client.*;
import com.rookie.constants.Constants;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer2 {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
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
        //4. 消费消息
        DefaultConsumer consumer = new DefaultConsumer(channel){
            //从队列中收到消息, 就会执行的方法
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("接收到消息:"+ new String(body));
            }
        };
        channel.basicConsume(Constants.WORK_QUEUE, true, consumer);

        //等待2s，让消费者有足够的时间消费消息
//        Thread.sleep(2000);
//        //6. 资源释放
//        connection.close();
    }
}

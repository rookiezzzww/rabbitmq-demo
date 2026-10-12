package com.rookie.rpc;

import com.rabbitmq.client.*;
import com.rookie.constants.Constants;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RpcClient {
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
        //3.声明请求和响应队列
        channel.queueDeclare(Constants.RPC_REQUEST_QUEUE, true, false, false, null);
        channel.queueDeclare(Constants.RPC_RESPONSE_QUEUE, true, false, false, null);
        //4. 发送请求
        String msg = "hello rpc...";
        String correlationId = UUID.randomUUID().toString();
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .replyTo(Constants.RPC_RESPONSE_QUEUE)
                .build();
        channel.basicPublish("", Constants.RPC_REQUEST_QUEUE, props, msg.getBytes());

        //5. 接收响应
        //使用阻塞队列来接受响应信息
        /**
         * 1.主线程注册消费者后，走到 response.take()
         * 2. 此时队列是空的，主线程阻塞在这里
         * 3. 服务端返回响应，回调线程执行 handleDelivery
         * 4. 回调线程把结果 offer 进队列
         * 5. 主线程被唤醒，take() 拿到结果，继续打印
         */
        final BlockingQueue response = new ArrayBlockingQueue(1);
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String response_msg = new String(body);
                System.out.println("收到server端响应：" + response_msg);
                if(properties.getCorrelationId().equals(correlationId)){
                    response.offer(response_msg);//将响应信息放入阻塞队列
                }
            }
        };
        channel.basicConsume(Constants.RPC_RESPONSE_QUEUE, true, consumer);

        System.out.println("[RPC Client 响应结果]:"+ response.take());//从阻塞队列中取出响应信息
    }
}

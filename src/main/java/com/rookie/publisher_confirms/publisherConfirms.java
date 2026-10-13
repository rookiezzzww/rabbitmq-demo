package com.rookie.publisher_confirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rookie.constants.Constants;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

public class publisherConfirms {
    private static final int MESSAGE_COUNT = 10000;
    public static void main(String[] args) throws Exception {
        //单独测试发布确认
        publisherConfirmsIndividual();
        //批量测试发布确认
        publisherConfirmsBatch();
        //异步测试发布确认
        publisherConfirmsAsync();
    }
    //建立连接
    private static Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(Constants.HOST);
        connectionFactory.setPort(Constants.PORT); //需要提前开放端口号
        connectionFactory.setUsername(Constants.USER_NAME);//账号
        connectionFactory.setPassword(Constants.PASSWORD);  //密码
        connectionFactory.setVirtualHost(Constants.VIRTUAL_HOST); //虚拟主机
        Connection connection = connectionFactory.newConnection();
        return connection;
    }

    private static void publisherConfirmsIndividual() throws Exception {
        try(Connection connection = getConnection()) {
            //1.创建信道
            Channel channel = connection.createChannel();
            //2.开启发布确认
            channel.confirmSelect();
            //3.声明队列
            channel.queueDeclare(Constants.PUBLISHER_CONFIRMS_QUEUE1, true, false, false, null);
            //4.发送消息
            long start = System.currentTimeMillis();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String msg = "publisherConfirms" + i;
                channel.basicPublish("", Constants.PUBLISHER_CONFIRMS_QUEUE1, null, msg.getBytes());
                //等待确认
                channel.waitForConfirmsOrDie(5000);
            }
            long end = System.currentTimeMillis();
            System.out.println("publisherConfirmsIndividual发布"+MESSAGE_COUNT+"条消息确认耗时：" + (end - start) + "ms");
        }
    }
    private static void publisherConfirmsBatch() throws Exception {
        try(Connection connection = getConnection()) {
            //1.创建信道
            Channel channel = connection.createChannel();
            //2.开启发布确认
            channel.confirmSelect();
            //3.声明队列
            channel.queueDeclare(Constants.PUBLISHER_CONFIRMS_QUEUE2, true, false, false, null);
            //4.发送消息
            long start = System.currentTimeMillis();
            int batch = 200;
            int count = 0;
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String msg = "publisherConfirms" + i;
                channel.basicPublish("", Constants.PUBLISHER_CONFIRMS_QUEUE2, null, msg.getBytes());
                count++;
                //等待确认
                if(count == batch){
                    channel.waitForConfirmsOrDie(5000);
                    count = 0;}
            }
            if (count > 0)
                channel.waitForConfirmsOrDie(5000);
            long end = System.currentTimeMillis();
            System.out.println("publisherConfirmsBatch发布"+MESSAGE_COUNT+"条消息确认耗时：" + (end - start) + "ms");
        }
    }
    private static void publisherConfirmsAsync() throws Exception {
        try(Connection connection = getConnection()) {
            //1.创建信道
            Channel channel = connection.createChannel();
            //2.开启发布确认
            channel.confirmSelect();
            //3.声明队列
            channel.queueDeclare(Constants.PUBLISHER_CONFIRMS_QUEUE3, true, false, false, null);
            //有序集合,元素按照⾃然顺序进⾏排序,存储未confirm消息序号
            SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<>());
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) {
                    if (multiple) {
                        confirmSet.headSet(deliveryTag+1).clear();
                    } else {
                        confirmSet.remove(deliveryTag);
                    }
                }
                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.err.format("deliveryTag: %d, multiple: %b%n", deliveryTag, multiple);
                    if (multiple) {
                        confirmSet.headSet(deliveryTag+1).clear();
                    } else {
                        confirmSet.remove(deliveryTag);
                    }
                    //如果处理失败, 这⾥需要添加处理消息重发的场景. 此处代码省略
                }
            });
            //4.发送消息
            long start = System.currentTimeMillis();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String msg = "publisherConfirms" + i;
                //返回下一条即将被发布的消息的序号,所以它必须在 basicPublish() 之前调用,才能拿到
                long nextPublishSeqNo = channel.getNextPublishSeqNo();
                confirmSet.add(nextPublishSeqNo);
                channel.basicPublish("", Constants.PUBLISHER_CONFIRMS_QUEUE3, null, msg.getBytes());
                //等待确认
            }
            while (!confirmSet.isEmpty()) {
                Thread.sleep(10);
            }
            long end = System.currentTimeMillis();
            System.out.println("publisherConfirmsAsync发布"+MESSAGE_COUNT+"条消息确认耗时：" + (end - start) + "ms");
        }
    }
}

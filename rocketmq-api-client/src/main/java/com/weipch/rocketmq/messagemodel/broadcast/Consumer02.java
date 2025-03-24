package com.weipch.rocketmq.messagemodel.broadcast;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @Author 方唐镜
 * @Date 2025-03-24 14:42
 * @Description
 */
public class Consumer02 {


    public static void main(String[] args) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast_consumer_group");

        consumer.setNamesrvAddr("106.14.139.83:9876");
        // 设置消费起始位置策略：从最后消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 设置消费模式：广播模式消费
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 订阅消息
        consumer.subscribe("TopicTest", "model");
        // 注册并发消息监听器（消息处理回调）
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) ->{
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");

    }

}

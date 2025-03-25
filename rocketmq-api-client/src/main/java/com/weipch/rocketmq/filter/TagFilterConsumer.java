package com.weipch.rocketmq.filter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * @Author 方唐镜
 * @Date 2025-03-25 10:10
 * @Description
 */
public class TagFilterConsumer {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag_filter_consumer_group");
        consumer.setNamesrvAddr("106.14.139.83:9876");
        consumer.subscribe("TagFilterTest", "TagA || TagB");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context)->{
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");

    }


}

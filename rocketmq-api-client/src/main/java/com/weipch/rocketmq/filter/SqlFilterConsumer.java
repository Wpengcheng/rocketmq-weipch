package com.weipch.rocketmq.filter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

/**
 * @Author 方唐镜
 * @Date 2025-03-25 10:10
 * @Description
 */
public class SqlFilterConsumer {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag_filter_consumer_group");
        consumer.setNamesrvAddr("106.14.139.83:9876");
        // 消费者订阅消息，并指定TagA或TagB， 并且a属性的值在0到3之间
        String sqlExpression = "(TAGS is not null and TAGS in ('TagA','TagB'))" + "and (a is not null and a between 0 and 3)";
        consumer.subscribe("SqlFilterTest", MessageSelector.bySql(sqlExpression));
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context)->{
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");

    }


}

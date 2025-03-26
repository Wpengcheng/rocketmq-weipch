package com.weipch.rocketmq.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @Author 方唐镜
 * @Date 2025-03-26 15:02
 * @Description
 */
public class DelayConsumer {

    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("delay_message_consumer_group");
        consumer.setNamesrvAddr("106.14.139.83:9876");
        consumer.subscribe("delayTopic", "TagA");

        consumer.registerMessageListener((MessageListenerOrderly) (msgs, consumeOrderlyContext) -> {
            // MessageListenerOrderly 接口保证单线程按顺序消费同一队列的消息
            for (MessageExt message : msgs) {
                System.out.println("消费消息: " + new String(message.getBody()));
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
    }

}

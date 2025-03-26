package com.weipch.rocketmq.batch;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author 方唐镜
 * @Date 2025-03-26 19:51
 * @Description
 */
public class BatchConsumer {

    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("batch_message_consumer_group");
        consumer.setNamesrvAddr("106.14.139.83:9876");
        consumer.subscribe("BatchTopic", "TagA");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }

}

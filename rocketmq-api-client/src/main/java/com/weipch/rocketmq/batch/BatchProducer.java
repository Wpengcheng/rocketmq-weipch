package com.weipch.rocketmq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author 方唐镜
 * @Date 2025-03-26 19:50
 * @Description
 */
public class BatchProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("quick_start_producer_group");
        producer.setNamesrvAddr("106.14.139.83:9876");
        producer.start();

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message msg = new Message("BatchTopic", "TagA","KEY_"+i, ("Hello Batch " + i).getBytes());
            messages.add(msg);
        }
        // 发送批量消息（自动分割超限批次）
        SendResult result = producer.send(messages);
        System.out.println("发送结果: " + result);
    }


}

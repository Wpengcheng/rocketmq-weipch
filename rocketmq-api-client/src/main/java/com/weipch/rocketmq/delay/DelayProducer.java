package com.weipch.rocketmq.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Author 方唐镜
 * @Date 2025-03-26 14:53
 * @Description
 */
public class DelayProducer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("quick_start_producer_group");
        producer.setNamesrvAddr("106.14.139.83:9876");
        producer.start();

        Message message = new Message("delayTopic", "TagA", "delay_message".getBytes(RemotingHelper.DEFAULT_CHARSET));

        // 设置延迟级别为 3（即延迟 10 秒）
        message.setDelayTimeLevel(3);


        SendResult sendResult = producer.send(message);
        System.out.printf("%s%n", sendResult);
        producer.shutdown();

    }
}

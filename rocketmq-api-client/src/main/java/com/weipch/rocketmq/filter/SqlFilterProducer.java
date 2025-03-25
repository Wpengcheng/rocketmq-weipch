package com.weipch.rocketmq.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Author 方唐镜
 * @Date 2025-03-25 10:04
 * @Description
 */
public class SqlFilterProducer {
    private final static int MESSAGE_COUNT = 6;

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("quick_start_producer_group");
        producer.setNamesrvAddr("106.14.139.83:9876");
        producer.start();

        String[] tags = new String[] {"TagA","TagB","TagC"};
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            try {
                Message message = new Message("SqlFilterTest", tags[i % tags.length], ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 设置消息属性a
                message.putUserProperty("a", String.valueOf(i));
                SendResult sendResult = producer.send(message);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();

    }


}

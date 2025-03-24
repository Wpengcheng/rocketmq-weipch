package com.weipch.rocketmq.quickstart;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @Author 方唐镜
 * @Date 2025-03-24 14:34
 * @Description
 */
public class Producer {

    private final static int MESSAGE_COUNT = 10;


    public static void main(String[] args) throws Exception {

        // 初始化消息生产者配置
        DefaultMQProducer producer = new DefaultMQProducer("quick_start_producer_group");
        producer.setNamesrvAddr("106.14.139.83:9876");
        producer.setSendMsgTimeout(10 * 1000);
        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            try {
                // 构建消息
                Message message = new Message("TopicTest", "TagA", ("Hello RocketMQ " + i).getBytes());
                // 单向发送消息，不需要等待服务端响应
                producer.sendOneway(message);
                // 同步发送
                SendResult sendResult = producer.send(message, 20 * 1000);
                System.out.printf("同步发送结果：%s%n", sendResult);

                // 异步发送，需要回调
                producer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("异步发送结果：%s%n", sendResult);
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        System.out.println(throwable.getMessage());
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        producer.shutdown();
    }

}

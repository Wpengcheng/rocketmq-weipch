package com.weipch.rocketmq.ordermessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @Author 方唐镜
 * @Date 2025-03-25 13:36
 * @Description
 */
public class OrderProducer {

    private final static int MESSAGE_COUNT = 10;

    public static void main(String[] args) throws Exception{
        // 初始化消息生产者配置
        DefaultMQProducer producer = new DefaultMQProducer("quick_start_producer_group");
        producer.setNamesrvAddr("106.14.139.83:9876");
        producer.start();
        // 假设10个订单，每个订单有5个步骤，需要保证每个订单的5个步骤顺序执行
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            int orderId = i;
            for (int j = 0; j < 5; j++) {
                Message message = new Message("orderTopic", "TagA", "KEY" + orderId, ("order_" + orderId + " step " + j).getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 同步发送（send 方法），异步发送无法保证顺序
                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    // 使用 MessageQueueSelector 实现队列选择逻辑，确保相同订单的消息进入同一队列
                    @Override
                    public MessageQueue select(List<MessageQueue> messageQueues, Message message, Object arg) {
                        Integer orderId = (Integer) arg;
                        int index = orderId % messageQueues.size();
                        return messageQueues.get(index); // 哈希选择队列
                    }
                }, orderId);
                System.out.printf("%s%n", sendResult);
            }
        }
        producer.shutdown();

    }

}

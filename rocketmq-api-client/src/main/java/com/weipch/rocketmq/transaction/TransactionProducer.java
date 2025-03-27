package com.weipch.rocketmq.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author 方唐镜
 * @Date 2025-03-27 10:59
 * @Description
 */
public class TransactionProducer {

    public static void main(String[] args) throws Exception{

        // 1. 创建事务生产者
        TransactionMQProducer producer = new TransactionMQProducer("quick_start_producer_group");
        producer.setNamesrvAddr("106.14.139.83:9876");

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000));
        producer.setExecutorService(threadPoolExecutor);
        // 2. 配置事务监听器
        producer.setTransactionListener(new TransactionListener() {
            // 执行本地事务：生产者执行本地业务逻辑，决定事务提交或回滚。
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                try{
                    String tags = msg.getTags();
                    // 模拟业务逻辑，例如数据库操作
                    if (doBusinessTransaction(tags)) {
                        System.out.println("执行本地事务提交，tag："+tags);
                        return LocalTransactionState.COMMIT_MESSAGE; // 提交事务
                    } else {
                        System.out.println("执行本地事务失败，tag："+tags);
                        return LocalTransactionState.ROLLBACK_MESSAGE; // 回滚事务
                    }
                }catch(Exception e){
                    // 触发回查
                    return LocalTransactionState.UNKNOW;
                }
            }

            // 事务状态回查（补偿机制）：若生产者未明确提交/回滚，Broker会通过回调接口检查事务状态
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                // 检查本地事务状态，例如查询数据库日志
                // 检查本地事务状态，例如查询数据库日志
                return checkTransactionStatus(msg.getTransactionId()) ?
                        LocalTransactionState.COMMIT_MESSAGE :
                        LocalTransactionState.ROLLBACK_MESSAGE;
            }
        });
        // 3. 启动生产者
        producer.start();
        // 4. 发送事务消息
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            try{
                Message msg = new Message("TransactionTopic", tags[i % tags.length], "KEY"+ i , "支付事务消息".getBytes());
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.println("半消息发送结果：" + sendResult.getSendStatus());
                Thread.sleep(10);
            }catch(MQClientException e){
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }

        // 5. 关闭生产者（生产环境需保持长连接）
        producer.shutdown();

    }

    // 模拟本地事务执行
    private static boolean doBusinessTransaction(String tags) {
        // 返回true表示业务成功，false表示失败
        return "TagA".equals(tags);
    }

    // 模拟事务状态回查
    private static boolean checkTransactionStatus(String transactionId) {
        // 根据事务ID查询本地事务状态
        return true;
    }
}

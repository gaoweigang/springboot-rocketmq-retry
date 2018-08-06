package com.gwg.demo.mq.common;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

public class MQAccessBuilder {
	
	
	//构建生产者
	public DefaultMQProducer defaultMQProducer(String producerGroup, String namesrvAddr){
		//生产者的组名
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(namesrvAddr);
        
        return producer;
	}
	
	
	//构建消费者
    public DefaultMQPushConsumer defaultMQPushConsumer(String consumerGroup, String namesrvAddr, String topic, String subExpression, ConsumeFromWhere consumeFromWhere) throws MQClientException{
    	//消费者的组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        //指定NameServer地址，多个地址以 ; 隔开
        consumer.setNamesrvAddr(namesrvAddr);
        //订阅PushTopic下Tag为push的消息
        consumer.subscribe(topic, subExpression);

        //设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
        //如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(consumeFromWhere);
        //consumer.registerMessageListener(messageListener);
        return consumer;
    }
}

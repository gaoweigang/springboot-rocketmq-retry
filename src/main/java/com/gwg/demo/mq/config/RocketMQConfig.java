package com.gwg.demo.mq.config;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.gwg.demo.mq.common.MQAccessBuilder;
import com.gwg.demo.mq.common.MessageListenerConcurrentlyImpl;
import com.gwg.demo.mq.process.UserMessageProcess;

@Configuration
public class RocketMQConfig {
	
	private static final Logger logger = LoggerFactory.getLogger(RocketMQConfig.class);
	
	@Value("${rocketmq.producer.producerGroup}")
	private String producerGroup;
	
	@Value("${rocketmq.consumer.consumerGroup}")
	private String consumerGroup;
	
	@Value("${rocketmq.namesrvAddr}")
	private String namesrvAddr;
	
	
	@Bean
	public MQAccessBuilder mqAccessBuilder(){
		logger.info("producerGroup:{}, consumerGroup:{}, namesrvAddr:{}", producerGroup, consumerGroup, namesrvAddr);
		return new MQAccessBuilder();
	}
	
	@Bean
	public DefaultMQProducer defaultMQProducer() throws MQClientException{
		logger.info("producerGroup:{}, consumerGroup:{}, namesrvAddr:{}", producerGroup, consumerGroup, namesrvAddr);
		DefaultMQProducer defaultMQProducer = mqAccessBuilder().defaultMQProducer(producerGroup, namesrvAddr);
		 /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
		defaultMQProducer.start();
		return defaultMQProducer;
	}
	
	
	@Bean 
	public DefaultMQPushConsumer defaultMQPushConsumer() throws MQClientException{
		DefaultMQPushConsumer consumer = mqAccessBuilder().defaultMQPushConsumer(consumerGroup, namesrvAddr, "TopicTest", "*", ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.setMessageListener(messageListenerConcurrently());
		consumer.start();//启动消费者监听
		return consumer;
	}

	/**
	 * 消息逻辑处理
	 */
	@Bean("userMessageProcess")
	public <T> MessageListenerConcurrently messageListenerConcurrently(){
		return new MessageListenerConcurrentlyImpl(new UserMessageProcess<T>());
	}
	
	

}

package com.gwg.demo.mq.service;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSON;
import com.gwg.demo.Application;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=Application.class)
public class RocketMQTest {
	
	private static final Logger logger = LoggerFactory.getLogger(RocketMQTest.class);
	
	@Autowired
	private DefaultMQProducer producer;
	
	
	@Test
	public void testProduceMessage() throws MQClientException, RemotingException, MQBrokerException, InterruptedException{
		for(int i = 0; i < 100; i++){
           //创建一个消息实例，包含 topic、tag 和 消息体,如下：topic 为 "TopicTest"，tag 为 "push"
		   String content = "hello gaoweigang-" + i;
           Message message = new Message("TopicTest", "tagA", content.getBytes());
           logger.info(" hello ：{}", JSON.toJSON(message));
           SendResult result = producer.send(message);
           logger.info("发送响应：MsgId:" + result.getMsgId() + "，发送状态:" + result.getSendStatus());
		}
	}	
	

}

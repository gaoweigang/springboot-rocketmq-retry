package com.gwg.demo.mq.common;

import java.util.List;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageListenerConcurrentlyImpl implements MessageListenerConcurrently{

	private static final Logger logger = LoggerFactory.getLogger(MessageListenerConcurrentlyImpl.class);
	
	private MessageProcess messageProcess;
	
	public MessageListenerConcurrentlyImpl(MessageProcess messageProcess) {
        this.messageProcess = messageProcess;
    }
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		  try {
			  logger.info("消费消息 start =============================线程id:{},消息数量：{}", Thread.currentThread().getId(), msgs.size());
              for (MessageExt messageExt : msgs) {
                  String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
            	  logger.info("消息：{}， 重试次数：{}", messageBody, messageExt.getReconsumeTimes());
                  DetailRes result = messageProcess.process(messageBody);
                  System.out.println("消费响应：msgId : " + messageExt.getMsgId() + ",  msgBody : " + messageBody);//输出消息内容
                  if(result.isSuccess){
                	  return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;//消费成功
                  }else{
                	  //消费失败 重试
                	  logger.info("消费失败重试 .......");
                	  return ConsumeConcurrentlyStatus.RECONSUME_LATER;//重试
                  }
              }
          } catch (Exception e) {
              e.printStackTrace();
              return ConsumeConcurrentlyStatus.RECONSUME_LATER; //稍后再试
          }
          return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
	}

}

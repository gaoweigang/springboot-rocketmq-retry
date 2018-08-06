package com.gwg.demo.mq.process;

import java.util.List;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class UserProccess implements MessageListenerConcurrently{

	private static final Logger logger = LoggerFactory.getLogger(UserProccess.class);
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		  try {
			  logger.info("消费消息 start ===========================================");
              for (MessageExt messageExt : msgs) {

                  String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                  logger.info("messageExt: {}" , messageBody);//输出消息内容

                  System.out.println("消费响应：msgId : " + messageExt.getMsgId() + ",  msgBody : " + messageBody);//输出消息内容
              }
          } catch (Exception e) {
              e.printStackTrace();
              return ConsumeConcurrentlyStatus.RECONSUME_LATER; //稍后再试
          }
          return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
	}

}

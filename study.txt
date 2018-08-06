参考：https://help.aliyun.com/document_detail/43490.html?spm=a2c4g.11186623.6.583.veJfUd

顺序消息的重试
对于顺序消息，当消费者消费消息失败后，MQ 会自动不断进行消息重试（每次间隔时间为 1 秒），这时，应用会出现消息消费被阻塞的情况。因此，建议您使用顺序消息时，务必保证应用能够及时监控并处理消费失败的情况，避免阻塞现象的发生。

无序消息的重试
对于无序消息（普通、定时、延时、事务消息），当消费者消费消息失败时，您可以通过设置返回状态达到消息重试的结果。

无序消息的重试只针对集群消费方式生效；广播方式不提供失败重试特性，即消费失败后，失败消息不再重试，继续消费新的消息。

注意：以下内容都只针对无序消息生效。

重试次数
MQ 默认允许每条消息最多重试 16 次，每次重试的间隔时间如下：

第几次重试	与上次重试的间隔时间	第几次重试	与上次重试的间隔时间
1	10 秒	9	7 分钟
2	30 秒	10	8 分钟
3	1 分钟	11	9 分钟
4	2 分钟	12	10 分钟
5	3 分钟	13	20 分钟
6	4 分钟	14	30 分钟
7	5 分钟	15	1 小时
8	6 分钟	16	2 小时
如果消息重试 16 次后仍然失败，消息将不再投递。如果严格按照上述重试时间间隔计算，某条消息在一直消费失败的前提下，将会在接下来的 4 小时 46 分钟之内进行 16 次重试，超过这个时间范围消息将不再重试投递。

注意： 一条消息无论重试多少次，这些重试消息的 Message ID 不会改变。

配置方式
消费失败后，重试配置方式
集群消费方式下，消息消费失败后期望消息重试，需要在消息监听器接口的实现中明确进行配置（三种方式任选一种）：

返回 Action.ReconsumeLater （推荐）
返回 Null
抛出异常
示例代码

public class MessageListenerImpl implements MessageListener {
    @Override
    public Action consume(Message message, ConsumeContext context) {
        //方法3：消息处理逻辑抛出异常，消息将重试
        doConsumeMessage(message);
        //方式1：返回 Action.ReconsumeLater，消息将重试
        return Action.ReconsumeLater;
        //方式2：返回 null，消息将重试
        return null;
        //方式3：直接抛出异常， 消息将重试
        throw new RuntimeException("Consumer Message exceotion");
    }
}
消费失败后，不重试配置方式
集群消费方式下，消息失败后期望消息不重试，需要捕获消费逻辑中可能抛出的异常，最终返回 Action.CommitMessage，此后这条消息将不会再重试。

示例代码

public class MessageListenerImpl implements MessageListener {
    @Override
    public Action consume(Message message, ConsumeContext context) {
        try {
            doConsumeMessage(message);
        } catch (Throwable e) {
            //捕获消费逻辑中的所有异常，并返回 Action.CommitMessage;
            return Action.CommitMessage;
        }
        //消息处理正常，直接返回 Action.CommitMessage;
        return Action.CommitMessage;
    }
}
自定义消息最大重试次数
自定义 MQ 客户端日志配置，请升级 TCP Java SDK 版本到1.2.2及以上。

MQ 允许 Consumer 启动的时候设置最大重试次数，重试时间间隔将按照如下策略：

最大重试次数小于等于16次，则重试时间间隔同上表描述。
最大重试次数大于16次，超过16次的重试时间间隔均为每次2小时。
配置方式如下：

Properties properties = new Properties();
//配置对应 Consumer ID 的最大消息重试次数为20 次
properties.put(PropertyKeyConst.MaxReconsumeTimes,"20");
Consumer consumer =ONSFactory.createConsumer(properties);
注意：

消息最大重试次数的设置对相同 Consumer ID 下的所有 Consumer 实例有效。
如果只对相同 Consumer ID 下两个 Consumer 实例中的其中一个设置了 MaxReconsumeTimes，那么该配置对两个 Consumer 实例均生效。
配置采用覆盖的方式生效，即最后启动的 Consumer 实例会覆盖之前的启动实例的配置。
获取消息重试次数
消费者收到消息后，可按照如下方式获取消息的重试次数：

public class MessageListenerImpl implements MessageListener {
    @Override
    public Action consume(Message message, ConsumeContext context) {
        //获取消息的重试次数
        System.out.println(message.getReconsumeTimes());
        return Action.CommitMessage;
    }
}

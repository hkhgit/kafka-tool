# kafka-tool

kafka辅助功能工具组件，基于springboot + apollo + mybatis-plus依赖

开箱即用:只需要简单配置即可实现kafka的自动补偿消费、动态启停消费者、动态调整消费者concurrency等功能。

点个star支持作者辛苦开源吧 谢谢❤❤ 

# 场景

对于kafka日常的业务使用，我们通常会遇到以下问题：
- **消息发送失败**时，希望自动保存并支持重试。
- 想根据业务需求**动态启停消费者**，消费者也可能多个，希望可以批量调整。
- **消费者消费速度慢**，又受制于kafka的分区数。
- **消息消费失败**时，默认重试10次就会提交消费点位导致消息丢失。原生自定义失败逻辑复杂度高，希望统一拥有自动消费失败的补偿机制。

# 功能 
- [x] kafka发送消息失败时自动落表，支持重试机制。
- [x] 基于apollo配置动态启停消费者，支持 全局/topic/自定义标签 三种级别 
- [x] 动态调整消费者concurrency。
- [x] 线程池加速消费，针对因分区数少，利用线程池来提高单个消费者的消息消费速度。
- [x] 消费失败重试，支持 本地/DB/kafka 三种补偿机制


`必看 注意事项：`

> 1、**使用前请确认是否有apollo配置，如果非apollo可以源码改造**
> 
> 2、**使用前请确认是否有mybatis-plus依赖**

## 动态启停消费者&调整concurrency
### 1. 全局关闭调整消费者(最高优先级):
| 配置项                                                 | 数据类型    | 默认值   | 描述                                   |
|-----------------------------------------------------|---------|-------|--------------------------------------|
| kafka-tool.adjust.all-stop                 | boolean | false | 全局关闭开关：控制全局消费者是否关闭， true 为关闭         |
| kafka-tool.adjust.all-stop.exclude.biz-tag | String  |       | 不受全局关闭开关影响的 bizTag ,多个bizTag使用 逗号分隔符 |
| kafka-tool.adjust.all-stop.exclude.topic   | String  |       | 不受全局关闭开关影响的 topic ,多个topic使用 逗号分隔符   |

⚠️⚠️⚠️注意⚠️⚠️⚠️:
**由于该开关为全局，打开全局关闭时需注意是否有使用自动补偿消费，
如果有使用自动补偿消费且使用kafka模式，需要自行判断是否开启补偿消费并将补偿队列配置排除项，否则自动补偿消费亦会被关闭。**

### 2. topic级别
| 配置项                                             | 数据类型    | 默认值   | 描述                                         |
|-------------------------------------------------|---------|----------|--------------------------------------------|
| kafka-tool.adjust.topic.${topic}.enabled        | boolean | 初始值   | 控制 listener container 是否开启。 false 为关闭      |
| kafka-tool.adjust.topic.${topic}.concurrency    | int     | 初始值   | 控制 listener container 的 concurrency。 0 为关闭 |


### 3. bizTag级别
通过注解`@BizAdjust`可以**批量控制**多个相同业务标签的消费者。
```java
  @BizAdjust(bizTag = "xxxTag")
  @KafkaListener(topics = "myTopic", groupId = "myGroup", containerFactory = "myFactory")
  public void method(List<ConsumerRecord<String, String>> recordList) {
    doSomething();
  }
```

| 配置项                                              | 数据类型  | 默认值    | 描述                                                                       |
|---------------------------------------------------|----------|--------|--------------------------------------------------------------------------|
| kafka-tool.adjust.${bizTag}.enabled               | boolean  | true   | 控制 bizTag 对应的 listener，true代表开启消费 bizTag 对应的 listener，false代表关闭          |
| kafka-tool.adjust.${bizTag}.${topic}.enabled      | boolean  | true   | 控制 bizTag 下 topic 对应的 listener 是否强制开启，使用该配置将忽略${bizTag}.enabled的情况 |
| kafka-tool.adjust.${bizTag}.${topic}.concurrency  | int      | 初始值  | 控制 listener container 的 concurrency。 0为关闭                                |

## 自动补偿消费

Spring Kafka 在客户端实现了 Retry Topic 和 DLT （Dead Letter Topic，死信队列）这两个功能。(2.7.x 以下版本不支持 Retry Topic)
这里与该功能的区别主要在于
1. 可以支持 本地重试/DB重试/kafka重试 三种模式，默认重试到达最大次数会落db保存(也可自定义)，方便查看处理
2. 对于老版本的spring-kafka友好

```yaml
kafka-tool:
  compensate:
    enabled: true
    kafka-clusters:
      # 这里即补偿集群的名称，@EnhancedKafkaListener对应的kafkaCluster名称
      origin:
        bootstrap-servers: IP_ADDRESS:9092,IP_ADDRESS:9092,IP_ADDRESS:9092
        topic: 补偿队列topic名称
        group-id: 补偿队列的group
```



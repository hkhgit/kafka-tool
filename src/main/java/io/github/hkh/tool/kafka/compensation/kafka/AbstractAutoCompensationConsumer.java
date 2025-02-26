package io.github.hkh.tool.kafka.compensation.kafka;

import io.github.hkh.tool.kafka.compensation.CompensationResult;
import io.github.hkh.tool.kafka.compensation.Compensator;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import io.github.hkh.tool.kafka.config.compensation.KafkaTypeCompensationProperties;
import io.github.hkh.tool.kafka.utils.JsonUtil;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import java.time.LocalDateTime;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/10
 */
@Slf4j
public abstract class AbstractAutoCompensationConsumer {


    protected String clusterName;

    protected KafkaCompensationProducer kafkaCompensationProducer;

    protected CompensationProperties compensationProperties;

    protected KafkaTypeCompensationProperties kafkaTypeCompensationProperties;


    public AbstractAutoCompensationConsumer(String clusterName, KafkaCompensationProducer kafkaCompensationProducer, CompensationProperties compensationProperties) {
        if (!compensationProperties.getClusters().containsKey(clusterName)) {
            throw new IllegalArgumentException("<kafka-tool>kafka补偿集群配置不存在, clusterName: " + clusterName);
        }
        this.clusterName = clusterName;
        this.kafkaCompensationProducer = kafkaCompensationProducer;
        this.kafkaTypeCompensationProperties = compensationProperties.getClusters().get(clusterName);
        this.compensationProperties = compensationProperties;
    }

    
    protected void doCompensate(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String message = record.value();
        try {
            CompensationMessage compensateMessage = JsonUtil.json2Bean(message, CompensationMessage.class);
            assert compensateMessage != null;
            log.info("<kafka-tool>Kafka自动补偿, 补偿队列{} 读取到一条消息，timestamp:{}", record.topic() , compensateMessage.getRecordTimestamp());
            if (!arriveTime(compensateMessage)) {
                log.info("<kafka-tool>Kafka自动补偿, 当前消息执行间隔未达到下次执行时间{}，延后处理, timestamp:{}", compensateMessage.getNextCompensateTime(), compensateMessage.getRecordTimestamp());
//                kafkaCompensationProducer.produce(compensateMessage);
                // 暂停监听器容器，等待下次
                pauseContainer();
                return;
            }
            CompensationResult compensationResult = Compensator.compensate(compensateMessage);
            // 补偿失败，且未到达最大补偿次数，重新入队
            if (!compensationResult.isCompensateSuccess() && !compensationResult.isArriveErrorLimit()) {
                log.info("<kafka-tool>Kafka自动补偿, 当前消息处理失败, nextCompensateTime:{}", compensateMessage.getNextCompensateTime());
                kafkaCompensationProducer.produce(compensateMessage);
            }
        } catch (Exception e) {
            log.error("<kafka-tool>Kafka自动补偿, 处理消息异常, 该条消息会被确认, 异常信息", e);
        }
        acknowledgment.acknowledge();
    }

    /**
     * 暂停监听器容器, 需要由定时任务恢复
     * <p>
     * 低版本的 spring-kafka 无法按 partition 暂停, 只能暂停整个容器;
     */
    private void pauseContainer() {
        log.info("<kafka-tool>Kafka自动补偿, 开始暂停监听器容器");
        try {
            String compensateListenerBeanName = KafkaCompensationClusterBeanConfigurer.getCompensateListenerBeanName(clusterName);
            MessageListenerContainer concurrentMessageListenerContainer = SpringContextUtil.getBean(compensateListenerBeanName, MessageListenerContainer.class);
            if (concurrentMessageListenerContainer.isPauseRequested()) {
                log.info("<kafka-tool>Kafka自动补偿, 监听器容器已暂停, 无需再次操作");
            } else {
                concurrentMessageListenerContainer.pause();
                log.info("<kafka-tool>Kafka自动补偿, 暂停监听器容器完成, 等待定时任务唤醒");
            }
        } catch (Exception e) {
            log.error("<kafka-tool>Kafka自动补偿, 获取或暂停监听器容器异常, 异常信息: {}", ExceptionUtils.getStackTrace(e));
        }
    }

    private boolean arriveTime(CompensationMessage compensationMessage) {
        return compensationMessage.getNextCompensateTime().isBefore(LocalDateTime.now());
    }





}

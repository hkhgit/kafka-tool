package io.github.hkh.tool.kafka.compensation.strategy;

import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.kafka.KafkaCompensationProducer;
import lombok.extern.slf4j.Slf4j;

/**
 * Description: kafka补偿策略
 *
 * @author kai
 * @date 2025/1/17
 */
@Slf4j
public class KafkaCompensationStrategy implements CompensationStrategy{



    @Override
    public CompensationTypeEnum getCompensationType() {
        return CompensationTypeEnum.Kafka;
    }

    @Override
    public CompensationStrategyResult execute(Throwable proceedException, CompensationContext compensationContext) {
        try {
            // 构建补偿消息
            CompensationMessage compensationMessage = compensationContext.getCompensationMessage();
            String kafkaCluster = compensationContext.getKafkaCluster();
            compensationMessage.setCompensationCluster(kafkaCluster);
            // 发送补偿消息
            KafkaCompensationProducer kafkaCompensationProducer = compensationContext.getKafkaCompensationProducer();
            kafkaCompensationProducer.produce(compensationMessage);
            log.info("<kafka-tool>异常消费消息推送到补偿队列, 新的消息体:[{}], 异常:{}", compensationMessage, proceedException);
            return new CompensationStrategyResult(true);
        }catch (Exception e){
            log.error("<kafka-tool>补偿消息发送失败, 补偿消息:[{}], 异常:{}", compensationContext.getCompensationMessage(), e);
            return new CompensationStrategyResult(false);
        }
    }


}

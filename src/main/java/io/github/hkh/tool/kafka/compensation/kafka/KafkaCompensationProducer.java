package io.github.hkh.tool.kafka.compensation.kafka;

import io.github.hkh.tool.kafka.config.compensation.KafkaTypeCompensationProperties;
import io.github.hkh.tool.kafka.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.sender.ReliableSender;

/**
 * Description: kafka补偿消息推送者
 *
 * @author kai
 * @date 2025/1/4
 */
@Slf4j
public class KafkaCompensationProducer {

    private final KafkaTemplate kafkaTemplate;

    private final String kafkaTemplateBeanName;

    private final KafkaTypeCompensationProperties kafkaTypeCompensationProperties;

    private final ReliableSender reliableSender;

    public KafkaCompensationProducer(KafkaTemplate kafkaTemplate, String kafkaTemplateBeanName, KafkaTypeCompensationProperties kafkaTypeCompensationProperties, ReliableSender reliableSender) {
        if (kafkaTemplate == null) {
            throw new IllegalArgumentException("kafkaTemplate is null");
        }
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplateBeanName = kafkaTemplateBeanName;
        this.kafkaTypeCompensationProperties = kafkaTypeCompensationProperties;
        this.reliableSender = reliableSender;
    }

    public void produce(CompensationMessage compensationMessage) {
        String topic = kafkaTypeCompensationProperties.getTopic();
        try {
            String compensationMsg = JsonUtil.bean2Json(compensationMessage);

            //补偿消息保持原消息的key以保证消息顺序性
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,  compensationMessage.getRecordKey(), compensationMsg);
            if (MapUtils.isNotEmpty(compensationMessage.getRecordHeaders())) {
                compensationMessage.getRecordHeaders().forEach((k, v) -> producerRecord.headers().add(k, v.getBytes()));
            }
            reliableSender.send(kafkaTemplate, kafkaTemplateBeanName, producerRecord);
        } catch (Exception e) {
            log.error("send kafka msg:{} to topic failed: ex:{}", compensationMessage, e);
            throw e;
        }
    }



}

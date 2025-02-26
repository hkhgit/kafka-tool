package io.github.hkh.tool.kafka.compensation.kafka;

import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Description: 单条补偿消费者
 *
 * @author kai
 * @date 2025/1/15
 */
public class SingleCompensationConsumer extends AbstractAutoCompensationConsumer implements AcknowledgingMessageListener<String,String> {

    public SingleCompensationConsumer(String clusterName, KafkaCompensationProducer kafkaCompensationProducer, CompensationProperties compensationProperties) {
        super(clusterName, kafkaCompensationProducer, compensationProperties);
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        doCompensate(data, acknowledgment);
    }
}

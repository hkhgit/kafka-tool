package io.github.hkh.tool.kafka.compensation.kafka;

import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

/**
 * Description: 批量补偿消费者
 *
 * @author kai
 * @date 2025/1/12
 */
public class BatchCompensationConsumer extends AbstractAutoCompensationConsumer implements BatchAcknowledgingMessageListener<String,String> {

    public BatchCompensationConsumer(String clusterName, KafkaCompensationProducer kafkaCompensationProducer, CompensationProperties compensationProperties) {
        super(clusterName, kafkaCompensationProducer, compensationProperties);
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data, Acknowledgment acknowledgment) {
        data.forEach(record -> {
            doCompensate(record, acknowledgment);
        });
    }
}

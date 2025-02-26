package io.github.hkh.tool.kafka.compensation;

import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;
import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import io.github.hkh.tool.kafka.config.compensation.KafkaTypeCompensationProperties;
import lombok.Data;

import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.handler.CompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.handler.CompensationHandler;
import io.github.hkh.tool.kafka.compensation.kafka.KafkaCompensationProducer;

/**
 * Description: 补偿上下文
 *
 * @author kai
 * @date 2025/1/17
 */
@Data
public class CompensationContext {

    CompensationProperties compensationProperties;

    CompensationTypeEnum compensateType;

    int compensateLimitMax;

    int compensateInterval;

    CompensationMessage compensationMessage;

    CompensationHandler compensationHandler;

    CompensationErrorHandler compensationErrorHandler;

    //-------------------------kafka类型补偿属性-------------------------
    String kafkaCluster;

    KafkaTypeCompensationProperties kafkaTypeCompensationProperties;

    KafkaCompensationProducer kafkaCompensationProducer;
}

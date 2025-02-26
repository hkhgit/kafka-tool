package io.github.hkh.tool.kafka.compensation.strategy;

import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;

/**
 * Description: 重试策略工厂
 *
 * @author kai
 * @date 2025/1/17
 */
public class CompensationStrategyFactory {

    public static CompensationStrategy getStrategy(CompensationContext retryContext) {
        CompensationTypeEnum compensationType = retryContext.getCompensateType();
        switch (compensationType) {
            case Local:
                return new LocalCompensationStrategy();
            case DB:
                return new DBCompensationStrategy();
            case Kafka:
                return new KafkaCompensationStrategy();
            default:
                throw new IllegalArgumentException("<kafka-tool>不支持的重试类型" + compensationType);
        }
    }
}

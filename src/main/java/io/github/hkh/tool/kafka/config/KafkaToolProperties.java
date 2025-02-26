package io.github.hkh.tool.kafka.config;

import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Description: kafka-tool配置类
 *
 * @author kai
 * @date 2024/12/30
 */
@ConfigurationProperties(prefix = "kafka-tool")
@Data
public class KafkaToolProperties {

    /**
     * kafka-tool 动态调整
     */
    private final AdjustConsumerProperties adjust = new AdjustConsumerProperties();

    /**
     * kafka-tool 补偿
     */
    private final CompensationProperties compensate = new CompensationProperties();

    @Data
    public static class AdjustConsumerProperties{

        /**
         * 动态调整消费者配置是否启用
         */
        private boolean enabled = false;

        /**
         * 环境标识(可用于灰度切换，区分不同环境的消费者配置)
         */
        private String env;

    }
}

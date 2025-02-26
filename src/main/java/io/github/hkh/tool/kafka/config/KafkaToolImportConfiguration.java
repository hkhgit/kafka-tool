package io.github.hkh.tool.kafka.config;

import io.github.hkh.tool.kafka.compensation.KafkaEnhanceAutoConfiguration;
import io.github.hkh.tool.kafka.concurrency.AdjustConcurrencyAutoConfiguration;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/9
 */
@EnableConfigurationProperties(value = KafkaToolProperties.class)
@MapperScan(basePackages = {"io.github.hkh.tool.kafka.**.mapper"})
@Import({
        AdjustConcurrencyAutoConfiguration.class,
        KafkaEnhanceAutoConfiguration.class,
})
public class KafkaToolImportConfiguration {
}

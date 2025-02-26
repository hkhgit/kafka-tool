package io.github.hkh.tool.kafka.compensation;

import io.github.hkh.tool.kafka.compensation.handler.DefaultCompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.kafka.KafkaCompensationClusterBeanConfigurer;
import io.github.hkh.tool.kafka.compensation.scheduler.DBCompensationScheduler;
import io.github.hkh.tool.kafka.compensation.service.PreserveCompensationMessageService;
import io.github.hkh.tool.kafka.compensation.service.PreserveCompensationMessageServiceImpl;
import io.github.hkh.tool.kafka.config.KafkaToolProperties;
import io.github.hkh.tool.kafka.enhance.aspect.EnhanceAspect;
import io.github.hkh.tool.kafka.sender.ReliableSender;
import io.github.hkh.tool.kafka.sender.service.DefaultPreserveSendMsgServiceImpl;
import io.github.hkh.tool.kafka.sender.service.PreserveSendMsgService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/3
 */
@ConditionalOnProperty(value = "kafka-tool.compensate.enabled", havingValue = "true")
public class KafkaEnhanceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public PreserveSendMsgService preserveSendMsgService() {
        return new DefaultPreserveSendMsgServiceImpl();
    }


    @Bean
    @ConditionalOnMissingBean
    public ReliableSender reliableSender(PreserveSendMsgService preserveSendMsgService){
        return new ReliableSender(preserveSendMsgService);
    }


    /**
     * Description: 创建KafkaClusterConfigurer用于配置kafka集群
     *
     * @param environment spring运行环境
     * @return KafkaClusterConfigurer
     */
    @Bean
    public KafkaCompensationClusterBeanConfigurer compensateClusterBeanConfigurer(Environment environment) {
        return new KafkaCompensationClusterBeanConfigurer(environment);
    }


    @Bean
    @ConditionalOnMissingBean
    public PreserveCompensationMessageService compensateErrorPreserveService() {
        return new PreserveCompensationMessageServiceImpl();
    }

    @Bean
    @ConditionalOnMissingBean
    public DBCompensationScheduler dbCompensationScheduler(PreserveCompensationMessageService preserveCompensationMessageService, KafkaToolProperties kafkaToolProperties) {
        return new DBCompensationScheduler(preserveCompensationMessageService, kafkaToolProperties.getCompensate());
    }

    @Bean
    public DefaultCompensationErrorHandler defaultCompensationErrorHandler(PreserveCompensationMessageService preserveCompensationMessageService) {
        return new DefaultCompensationErrorHandler(preserveCompensationMessageService);
    }

    @Bean
    public EnhanceAspect kafkaConsumerEnhanceAspect(PreserveCompensationMessageService preserveCompensationMessageService, KafkaToolProperties kafkaToolProperties) {
        return new EnhanceAspect(preserveCompensationMessageService, kafkaToolProperties);
    }
}

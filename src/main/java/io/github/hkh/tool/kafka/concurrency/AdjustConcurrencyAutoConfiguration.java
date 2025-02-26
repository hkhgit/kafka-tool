package io.github.hkh.tool.kafka.concurrency;

import io.github.hkh.tool.kafka.concurrency.matcher.AllStopMatcher;
import io.github.hkh.tool.kafka.concurrency.matcher.BizTagConcurrencyMatcher;
import io.github.hkh.tool.kafka.concurrency.matcher.TopicConcurrencyMatcher;
import io.github.hkh.tool.kafka.config.init.BizAdjustInitializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Description: 自动装配类
 *
 * @author kai
 * @date 2024/12/28
 */
@ConditionalOnProperty(value = "kafka-tool.adjust.enabled", havingValue = "true")
public class AdjustConcurrencyAutoConfiguration {

    /**
     * Description: 动态调整消费者数量初始化
     */
    @Bean
    @ConditionalOnMissingBean
    public ConcurrencyAdjustManager consumerAdjustManager() {
        return new ConcurrencyAdjustManager();
    }

    /**
     * Description: 动态调整业务标签消费者数量初始化
     */
    @Bean
    @ConditionalOnMissingBean
    public BizAdjustInitializer bizAdjustInitializer(){
        return new BizAdjustInitializer();
    }

    /**
     * Description: 调整topic级别
     */
    @Bean
    @ConditionalOnMissingBean
    public TopicConcurrencyMatcher oneTopicAdjustMatcher() {
        return new TopicConcurrencyMatcher();
    }

    // TODO: 2024/12/30 待实现 调整topic+group级别

    /**
     * Description: 调整所有消费者
     */
    @Bean
    @ConditionalOnMissingBean
    public AllStopMatcher allStopAdjustMatcher() {
        return new AllStopMatcher();
    }

    /**
     * Description: 调整bizTag消费者
     */
    @Bean
    @ConditionalOnMissingBean
    public BizTagConcurrencyMatcher bizTagAdjustMatcher() {
        return new BizTagConcurrencyMatcher();
    }

}

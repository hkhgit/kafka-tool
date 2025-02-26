package io.github.hkh.tool.kafka.concurrency;

import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import io.github.hkh.tool.kafka.concurrency.matcher.AdjustMatcher;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.github.hkh.tool.kafka.config.KafkaToolProperties;
import io.github.hkh.tool.kafka.utils.KafkaConsumerUtil;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Description: 并发调整管理类
 *
 * @author kai
 * @date 2024/12/28
 */
@Slf4j
public class ConcurrencyAdjustManager implements ApplicationRunner, BeanPostProcessor, ConfigChangeListener {


    /**
     * 消费者调整匹配器
     */
    private final List<AdjustMatcher> consumerAdjustMatchers = new CopyOnWriteArrayList<>();

//    @PostConstruct
    public void listenApollo() {
        // 监听apollo配置变动，默认 namespace: application
        String property = SpringContextUtil.getProperty("apollo.bootstrap.namespaces");
        log.info("<kafka-tool> 监听 Apollo 配置变化[{}]", property);
        if (StringUtils.isBlank(property)) {
            ConfigService.getAppConfig().addChangeListener(this);
        }else {
            String[] namespaces = property.split(",");
            for (String namespace : namespaces) {
                ConfigService.getConfig(namespace).addChangeListener(this);
            }
        }
    }


    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof ConcurrentKafkaListenerContainerFactory) {
            ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory = (ConcurrentKafkaListenerContainerFactory<?, ?>) bean;
            // 默认关闭 kafka 监听器容器，后面统一开启
            containerFactory.setAutoStartup(false);
            log.info("<kafka-tool> Kafka 消费者数量初始化开始，关闭监听器 factory：{}", beanName);
        }
        if (bean instanceof AdjustMatcher) {
            consumerAdjustMatchers.add((AdjustMatcher) bean);
        }
        return BeanPostProcessor.super.postProcessBeforeInitialization(bean, beanName);
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        listenApollo();
        if (CollectionUtils.isNotEmpty(consumerAdjustMatchers)) {
            //排序
            consumerAdjustMatchers.sort(Comparator.comparingInt(AdjustMatcher::getAdjustOrder).reversed());
        }
        Collection<MessageListenerContainer> containers = KafkaConsumerUtil.getAllListenerContainers();
        Set<String> propertyNames = ConfigService.getAppConfig().getPropertyNames();

        if (CollectionUtils.isEmpty(containers)) {
            return;
        }
        //初始化 各维度配置
        for (AdjustMatcher consumerAdjustMatcher : consumerAdjustMatchers) {
            consumerAdjustMatcher.initConsumerConfig(propertyNames);
        }
        adjustConsumer(containers,true);
        log.info("<kafka-tool> Kafka 消费者数量初始化结束");
    }

    @Override
    public void onChange(ConfigChangeEvent configChangeEvent) {
        Set<String> changedKeys = configChangeEvent.changedKeys();
        Collection<MessageListenerContainer> containers = KafkaConsumerUtil.getAllListenerContainers();

        if (CollectionUtils.isEmpty(containers) || CollectionUtils.isEmpty(changedKeys)) {
            return;
        }
        log.info("<kafka-tool> 监听到 Apollo 配置变化：{}", changedKeys);
        //初始化 consumerConcurrencyAdjuster
        for (AdjustMatcher consumerAdjustMatcher : consumerAdjustMatchers) {
            consumerAdjustMatcher.notifyConfigChange(configChangeEvent);
        }

        //根据apollo配置调整消费者数量
        adjustConsumer(containers,false);

        //初始化 consumerConcurrencyAdjuster
        for (AdjustMatcher consumerAdjustMatcher : consumerAdjustMatchers) {
            consumerAdjustMatcher.notifyConfigChangeDone();
        }
    }



    private void adjustConsumer(Collection<MessageListenerContainer> containers, boolean isInit) {
        //根据apollo配置调整消费者数量
        for (MessageListenerContainer container : containers) {
            try {
                ConcurrentMessageListenerContainer concurrentMessageListenerContainer = (ConcurrentMessageListenerContainer) container;
                String[] topics = concurrentMessageListenerContainer.getContainerProperties().getTopics();
                String groupId = concurrentMessageListenerContainer.getGroupId();
                assert topics != null;

                int currentConcurrency;
                if(isInit){
                    currentConcurrency = concurrentMessageListenerContainer.getConcurrency();
                }else{
                    currentConcurrency = concurrentMessageListenerContainer.isRunning() ? concurrentMessageListenerContainer.getConcurrency() : 0;
                }

                //是否需要启动标识
                boolean needStartFlag = true;
                Integer matchCount = getMatchCount(isInit, concurrentMessageListenerContainer);

                //设置消费者数量,仅一个维度设置一次
                if (matchCount != currentConcurrency){
                    log.info("<kafka-tool> Kafka 消费者数量调整: topic:{}, groupId:{}, 原concurrency:{}, 变更concurrency:{}",
                            Arrays.asList(topics), groupId, currentConcurrency, matchCount);
                    //apollo配置变更时，先停止容器
                    concurrentMessageListenerContainer.stop();
                }

                if (matchCount > 0){
                    // 大于 0 的时候才需要设置
                    concurrentMessageListenerContainer.setConcurrency(matchCount);
                }else {
                    needStartFlag = false;
                }
                for (String topic : topics) {
                    AdjustContext.putTopicGroupConcurrency(topic, groupId, matchCount);
                }


                if (needStartFlag) {
                    concurrentMessageListenerContainer.start();
                }
            } catch (Exception e) {
                log.error("<kafka-tool> Kafka 消费者动态调整，异常：{}", ExceptionUtils.getStackTrace(e));
            }
        }
    }

    private Integer getMatchCount(boolean isInit, ConcurrentMessageListenerContainer concurrentMessageListenerContainer) {
        Integer matchCount = null;
        for (AdjustMatcher consumerAdjustMatcher : consumerAdjustMatchers) {
            //获取apollo配置
            matchCount = consumerAdjustMatcher.getMatchConcurrency(concurrentMessageListenerContainer, isInit);
            //没有命中配置,则为原生container配置
            if (matchCount != null) {
                break;
            }
        }
        if (matchCount == null){
            matchCount = concurrentMessageListenerContainer.getConcurrency();
        }
        return matchCount;
    }
}

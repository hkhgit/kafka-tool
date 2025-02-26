package io.github.hkh.tool.kafka.concurrency.matcher;

import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import java.util.Set;

/**
 * Description:
 *
 * @author kai
 * @date 2024/12/28
 */
public class TopicConcurrencyMatcher implements AdjustMatcher{

    private static final String TOPIC_ADJUST_KEY = "kafka-tool.adjust.topic";

    private static final String ENABLED_SUFFIX = "enabled";

    private static final String CONCURRENCY_SUFFIX = "concurrency";

    private static final String PROPERTIES_SUFFIX = "properties";


    /**
     * Description: 获取topic级别是否开启key
     * @param topic topic
     * @return kafka-tool.adjust.topic.${topic}.enabled
     */
    private String getTopicAdjustEnabledKey(String topic){
        return  getAdjustDynamicKey() + "." + topic + "." + ENABLED_SUFFIX;
    }


    /**
     * Description: 获取topic级别消费者线程数key
     * @param topic topic
     * @return kafka-tool.adjust.topic.${topic}.concurrency
     */
    private String getTopicAdjustConcurrencyKey(String topic){
        return  getAdjustDynamicKey() + "." + topic + "." + CONCURRENCY_SUFFIX;
    }

    /**
     * Description: 获取topic级别消费者属性key
     * @param topic topic
     * @return kafka-tool.adjust.topic.${topic}.properties
     */
    private String getTopicAdjustPropertiesKey(String topic){
        return  getAdjustDynamicKey() + "." + topic + "." + PROPERTIES_SUFFIX;
    }

    @Override
    public String getAdjustKey() {
        return TOPIC_ADJUST_KEY;
    }

    @Override
    public void initConsumerConfig(Set<String> propertyNames) {

    }

    @Override
    public Integer getMatchConcurrency(ConcurrentMessageListenerContainer listenerContainer, boolean isInit) {
        String[] topics = listenerContainer.getContainerProperties().getTopics();
        for (String topic : topics) {
            String topicAdjustEnabledKey = getTopicAdjustEnabledKey(topic);
            String topicAdjustConcurrencyKey = getTopicAdjustConcurrencyKey(topic);
            Boolean topicEnabled = SpringContextUtil.getEnvironment().getProperty(topicAdjustEnabledKey, Boolean.class, Boolean.TRUE);
            if (!topicEnabled){
                return 0;
            }
            return SpringContextUtil.getEnvironment().getProperty(topicAdjustConcurrencyKey, Integer.class);
        }
        return null;
    }

    @Override
    public void notifyConfigChange(ConfigChangeEvent changeEvent) {

    }

}

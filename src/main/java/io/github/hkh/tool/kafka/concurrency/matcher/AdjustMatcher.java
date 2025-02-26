package io.github.hkh.tool.kafka.concurrency.matcher;

import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import java.util.Set;

/**
 * Description: 调整匹配器
 *
 * @author kai
 * @date 2024/12/30
 */
public interface AdjustMatcher {


    String ADJUST_PRE_KEY = "kafka-tool.adjust.env";

    String SEPARATOR = ".";

    /**
     * Description: 调整优先级,大至小排序
     *
     * @return int
     */
    default int getAdjustOrder() {
        return 0;
    }

    /**
     * Description: 获取apollo默认配置key
     *
     * @return String
     */
    String getAdjustKey();

    /**
     * Description: 获取apollo完整配置key
     *
     * @return String
     */
    default String getAdjustDynamicKey() {
        String prefix = SpringContextUtil.getProperty(ADJUST_PRE_KEY, StringUtils.EMPTY);
        if (StringUtils.isNotBlank(prefix) && !prefix.endsWith(SEPARATOR)) {
            prefix = prefix + SEPARATOR;
        }
        return prefix + getAdjustKey();
    }

    /**
     * Description: 获取apollo完整配置key
     *
     * @param key 配置key
     * @return String
     */
    default String getAdjustDynamicKey(String key){
        String prefix = SpringContextUtil.getProperty(ADJUST_PRE_KEY, StringUtils.EMPTY);
        if (StringUtils.isNotBlank(prefix) && !prefix.endsWith(SEPARATOR)) {
            prefix = prefix + SEPARATOR;
        }
        return prefix + key;
    }

    /**
     * Description: 初始化apollo配置
     *
     * @author kai
     * @date  2024/12/30
     * @param propertyNames 配置key
     **/
    void initConsumerConfig(Set<String> propertyNames);


    /**
     * Description: 获取apollo配置维度(topic/topic+group) -> 消费者数量
     *
     * @param listenerContainer 监听器容器
     * @param isInit 是否初始化
     * @return Integer
     */
    Integer getMatchConcurrency(ConcurrentMessageListenerContainer listenerContainer, boolean isInit);


    /**
     * Description: 通知配置变更
     *
     * @param changeEvent 配置变更事件
     */
    default void notifyConfigChange(ConfigChangeEvent changeEvent){}


    /**
     * Description: 通知配置变更完成
     */
    default void notifyConfigChangeDone(){}

}

package io.github.hkh.tool.kafka.concurrency.matcher;

import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.google.common.collect.Maps;
import io.github.hkh.tool.kafka.concurrency.AdjustContext;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Description:
 *
 * @author kai
 * @date 2024/12/28
 */
public class BizTagConcurrencyMatcher implements AdjustMatcher{

    private static final String ADJUST_KEY = "kafka-tool.adjust.biz-tag";

    private static final String ENABLED_SUFFIX = "enabled";

//    private static final String FORCE_SUFFIX = "forceon";

    private static final String CONCURRENCY_SUFFIX = "concurrency";

    /**
     * <业务tag, 消费者数量>
     */
    private static final Map<String, Boolean> BIZ_TAG_ENABLED_MAP = Maps.newConcurrentMap();

    /**
     * Description: 获取bizTag下是否开启key
     *
     * @param bizTag 业务标签
     * @return kafka-tool.adjust.biz-tag.${tagName}.enabled
     */
    private String getBizTagEnabledKey(String bizTag){
        return  getAdjustDynamicKey() + "." + bizTag + "." + ENABLED_SUFFIX;
    }

    /**
     * Description: 获取bizTag下topic是否强制开启key
     *
     * @param bizTag 业务标签
     * @param topic topic
     * @return kafka-tool.adjust.biz-tag.${tagName}.${topic}.forceon
     */
//    private String getBizTagTopicForceOnKey(String bizTag,String topic){
//        return  getAdjustDynamicKey() + "." + bizTag + "." + topic + "." + FORCE_SUFFIX;
//    }

    /**
     * Description: 获取bizTag下topic是否开启key
     *
     * @param bizTag 业务标签
     * @param topic topic
     * @return kafka-tool.adjust.biz-tag.${tagName}.${topic}.enabled
     */
    private String getBizTagTopicEnabledKey(String bizTag,String topic){
        return  getAdjustDynamicKey() + "." + bizTag + "." + topic + "." + ENABLED_SUFFIX;
    }

    /**
     * Description: 获取bizTag下topic消费数量key
     *
     * @param bizTag 业务标签
     * @param topic topic
     * @return kafka-tool.adjust.biz-tag.${tagName}.${topic}.concurrency
     */
    private String getBizTagTopicCurrencyKey(String bizTag,String topic){
        return  getAdjustDynamicKey() + "." + bizTag + "." + topic + "." + CONCURRENCY_SUFFIX;
    }

    @Override
    public String getAdjustKey() {
        return ADJUST_KEY;
    }

    @Override
    public void initConsumerConfig(Set<String> propertyNames) {
        Map<String, String> topicGroupBizTagAdjustMap = AdjustContext.getTopicGroupBizTagMap();
        topicGroupBizTagAdjustMap.values().forEach(bizTag -> BIZ_TAG_ENABLED_MAP.put(bizTag, SpringContextUtil.getEnvironment().getProperty(getBizTagEnabledKey(bizTag), Boolean.class, Boolean.TRUE)));
    }

    @Override
    public Integer getMatchConcurrency(ConcurrentMessageListenerContainer listenerContainer, boolean isInit) {
        String[] topics = listenerContainer.getContainerProperties().getTopics();
        String groupId = listenerContainer.getGroupId();
        String bizTag = AdjustContext.getTopicGroupBizTag(topics[0] + groupId);
        if(StringUtils.isBlank(bizTag)){
            return null;
        }
        //业务标签是否开启消费，默认开启
        Boolean isBizTagEnabled = BIZ_TAG_ENABLED_MAP.getOrDefault(bizTag, Boolean.TRUE);
        for (String topic : topics) {
            //bizTag下topic单独配置的消费数量
            String bizTagTopicKey = getBizTagTopicEnabledKey(bizTag, topic);
            String bizTagTopicCurrencyKey = getBizTagTopicCurrencyKey(bizTag, topic);
            //bizTag下topic是否单独指定开关
            Boolean isTopicEnabled;
            if(isBizTagEnabled){
                isTopicEnabled = SpringContextUtil.getEnvironment().getProperty(bizTagTopicKey, Boolean.class, Boolean.TRUE);
            }else{
                isTopicEnabled = SpringContextUtil.getEnvironment().getProperty(bizTagTopicKey, Boolean.class, Boolean.FALSE);
            }
            if (isTopicEnabled){
                return SpringContextUtil.getEnvironment().getProperty(bizTagTopicCurrencyKey, Integer.class);
            }else{
                return 0;
            }
        }
        return isBizTagEnabled ? null : 0;
    }

    @Override
    public void notifyConfigChange(ConfigChangeEvent changeEvent) {
        changeEvent.changedKeys().forEach(changeKey -> {
            //判断是否是业务tag配置
            Set<String> bizTagSets = BIZ_TAG_ENABLED_MAP.keySet();
            bizTagSets.forEach(bizTag -> {
                String bizTagEnabledKey = getBizTagEnabledKey(bizTag);
                if (StringUtils.equals(changeKey, bizTagEnabledKey)){
                    ConfigChange change = changeEvent.getChange(changeKey);
                    if (Objects.isNull(change.getNewValue())){
                        //为空默认是开启
                        BIZ_TAG_ENABLED_MAP.remove(bizTagEnabledKey);
                    }else {
                        BIZ_TAG_ENABLED_MAP.put(bizTag, Boolean.valueOf(change.getNewValue()));
                    }
                }
            });
        });
    }

}

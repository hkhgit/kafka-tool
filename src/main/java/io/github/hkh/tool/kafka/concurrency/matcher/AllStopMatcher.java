package io.github.hkh.tool.kafka.concurrency.matcher;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import io.github.hkh.tool.kafka.concurrency.AdjustContext;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Description: 关闭所有消费者匹配器
 *
 * @author kai
 * @date 2024/12/28
 */
public class AllStopMatcher implements AdjustMatcher{

    private static final String ADJUST_KEY = "kafka-tool.adjust.all-stop.enabled";

    private static final String EXCLUDE_TOPIC_KEY = "kafka-tool.adjust.all-stop.exclude.topic";

    private static final String EXCLUDE_BIZ_TAG_KEY = "kafka-tool.adjust.all-stop.exclude.biz-tag";

    private static boolean isAllConsumerStop = false;

    @Override
    public int getAdjustOrder() {
        return 9;
    }

    @Override
    public String getAdjustKey() {
        return ADJUST_KEY;
    }

    @Override
    public void initConsumerConfig(Set<String> propertyNames) {
        String apolloAdjustDynamicKey = getAdjustDynamicKey();
        String propertyVal = SpringContextUtil.getProperty(apolloAdjustDynamicKey, StringUtils.EMPTY);
        isAllConsumerStop = Boolean.parseBoolean(propertyVal);
    }

    @Override
    public Integer getMatchConcurrency(ConcurrentMessageListenerContainer listenerContainer, boolean isInit) {
        if (!isAllConsumerStop){
            return null;
        }
        String excludeTopicKey = getAdjustDynamicKey(EXCLUDE_TOPIC_KEY);
        String excludeBizTagKey = getAdjustDynamicKey(EXCLUDE_BIZ_TAG_KEY);
//        KafkaConsumerUtil
        String[] topics = listenerContainer.getContainerProperties().getTopics();
        assert topics != null;
        List<String> topicLists = Lists.newArrayList(topics);
        //获取apollo配置，这里允许两个维度排除 bizTag/topic
        String excludeBizTagConfig = SpringContextUtil.getProperty(excludeBizTagKey, StringUtils.EMPTY);
        //匹配是否排除bizTag
        if (StringUtils.isNotBlank(excludeBizTagConfig)){
            List<String> excludeBizTagList = Arrays.asList(excludeBizTagConfig.split(","));
            String groupId = listenerContainer.getGroupId();
            if (excludeBizTagList.contains(AdjustContext.getTopicGroupBizTag(topics[0] + groupId))){
                return null;
            }
        }

        String excludeTopicConfig = SpringContextUtil.getProperty(excludeTopicKey, StringUtils.EMPTY);
        //匹配是否排除topic
        if (StringUtils.isNotBlank(excludeTopicConfig)){
            List<String> excludeTopicList = Arrays.asList(excludeTopicConfig.split(","));
            if (!CollectionUtils.intersection(topicLists, excludeTopicList).isEmpty()) {
                return null;
            }
        }
        return 0;
    }

}

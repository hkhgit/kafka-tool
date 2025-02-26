package io.github.hkh.tool.kafka.concurrency;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Description: kafka并发调整Context
 *
 * @author kai
 * @date 2025/1/3
 */
public class AdjustContext {

    /**
     * 消费者topic+group -> 业务tag
     */
    private static final Map<String, String> TOPIC_GROUP_BIZ_TAG_MAP = Maps.newConcurrentMap();

    /**
     * topic -> 消费者数量
     */
    private static final Map<String,Integer> TOPIC_GROUP_CONCURRENCY_MAP = Maps.newConcurrentMap();

    public static void putTopicGroupBizTag(String topicGroup, String bizTag) {
        TOPIC_GROUP_BIZ_TAG_MAP.put(topicGroup, bizTag);
    }

    public static Map<String, String> getTopicGroupBizTagMap() {
        return TOPIC_GROUP_BIZ_TAG_MAP;
    }

    public static String getTopicGroupBizTag(String topicGroup) {
        return TOPIC_GROUP_BIZ_TAG_MAP.get(topicGroup);
    }

    private static String getTopicGroupKey(String topic, String groupId) {
        return topic + "-" + groupId;
    }


    public static void putTopicGroupConcurrency(String topic, String groupId, Integer concurrency) {
        TOPIC_GROUP_CONCURRENCY_MAP.put(getTopicGroupKey(topic, groupId), concurrency);
    }

    public static Map<String, Integer> getTopicConcurrencyMap() {
        return TOPIC_GROUP_CONCURRENCY_MAP;
    }

    public static Integer getTopicConcurrency(String topic, String groupId) {
        return TOPIC_GROUP_CONCURRENCY_MAP.get(getTopicGroupKey(topic, groupId));
    }


}

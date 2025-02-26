package io.github.hkh.tool.kafka.config.init;

import io.github.hkh.tool.kafka.concurrency.KafkaBizTag;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.annotation.KafkaListener;

import io.github.hkh.tool.kafka.concurrency.AdjustContext;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Description: 业务调整初始化器
 *
 * @author kai
 * @date 2025/1/3
 */
public class BizAdjustInitializer implements KafkaToolInitializer{

    @Override
    public void init(ContextRefreshedEvent event, Method method, KafkaListener listenerAnnotation) {
        KafkaBizTag bizAdjustAnnotation = AnnotationUtils.findAnnotation(method, KafkaBizTag.class);
        if (bizAdjustAnnotation == null) {
            return;
        }
        String bizTag = bizAdjustAnnotation.name();
        String[] topics = listenerAnnotation.topics();
        String groupId = listenerAnnotation.groupId();
        // topic+groupId,bizTag
        Arrays.stream(topics).forEach(topic -> {
            AdjustContext.putTopicGroupBizTag(topic + groupId, bizTag);
        });
    }
}

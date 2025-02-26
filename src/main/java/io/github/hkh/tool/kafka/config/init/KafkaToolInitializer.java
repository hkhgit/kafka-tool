package io.github.hkh.tool.kafka.config.init;

import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;

/**
 * Description: kafka-tool 初始化/检查 接口
 *
 * @author kai
 * @date 2025/1/3
 */
public interface KafkaToolInitializer {

    /**
     * 检查方法
     *
     * @param event              event
     * @param method             method
     * @param listenerAnnotation listenerAnnotation
     */
    void init(ContextRefreshedEvent event, Method method, KafkaListener listenerAnnotation);
}

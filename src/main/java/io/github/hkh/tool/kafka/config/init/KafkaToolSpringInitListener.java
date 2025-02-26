package io.github.hkh.tool.kafka.config.init;

import io.github.hkh.tool.kafka.config.KafkaToolContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Description: kafka-tool Spring监听器
 *
 * @author kai
 * @date 2025/1/3
 */
@Slf4j
public class KafkaToolSpringInitListener implements ApplicationListener<ContextRefreshedEvent> {


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        log.info("<kafka-tool>开始检查/初始化 @KafkaListener 方法");
        // 获取所有已初始化的bean
        String[] beanNames = event.getApplicationContext().getBeanDefinitionNames();
        for (String beanName : beanNames) {
            Object bean = event.getApplicationContext().getBean(beanName);
            Method[] methods = bean.getClass().getDeclaredMethods();
            for (Method method : methods) {
                KafkaListener listenerAnnotation = AnnotationUtils.findAnnotation(method, KafkaListener.class);
                if (listenerAnnotation == null) {
                    continue;
                }

                Map<String, KafkaToolInitializer> checkerMap = event.getApplicationContext().getBeansOfType(KafkaToolInitializer.class);
                if (MapUtils.isEmpty(checkerMap)){
                    return;
                }
                for (KafkaToolInitializer initializer : checkerMap.values()) {
                    initializer.init(event, method, listenerAnnotation);
                }
            }
        }
        log.info("<kafka-tool>检查/初始化 @KafkaListener 方法 完成");
        KafkaToolContext.setInitCheck(true);
    }
}

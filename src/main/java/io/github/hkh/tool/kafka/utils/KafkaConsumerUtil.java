package io.github.hkh.tool.kafka.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.hkh.tool.kafka.compensation.kafka.KafkaCompensationClusterBeanConfigurer;
import io.github.hkh.tool.kafka.config.KafkaToolProperties;
import io.github.hkh.tool.kafka.config.compensation.KafkaTypeCompensationProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.core.MethodParameter;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Description: kafka消费者工具类
 *
 * @author kai
 * @date 2025/1/4
 */
@Slf4j
public class KafkaConsumerUtil {

    public static Collection<MessageListenerContainer> getAllListenerContainers() {

        Collection<MessageListenerContainer> allContainers = Lists.newArrayList();
        //获取@KafkaListener注解的监听器
        KafkaListenerEndpointRegistry endpointRegistry = SpringContextUtil.getBean(KafkaListenerEndpointRegistry.class);
        Collection<MessageListenerContainer> containers = endpointRegistry.getListenerContainers();
        allContainers.addAll(containers);
        //获取补偿动态配置的监听器
        allContainers.addAll(getCompensationListenerContainers());
        return allContainers;
    }

    public static Collection<ConcurrentMessageListenerContainer> getCompensationListenerContainers() {
        KafkaToolProperties kafkaToolProperties = SpringContextUtil.getBean(KafkaToolProperties.class);
        //获取补偿动态配置的监听器，这里不受kafkaListenerEndpointRegistry管理，而是spring管理
        Map<String, KafkaTypeCompensationProperties> compensationClusters = kafkaToolProperties.getCompensate().getClusters();
        if (MapUtils.isEmpty(compensationClusters)){
            return CollectionUtils.emptyCollection();
        }
        Collection<ConcurrentMessageListenerContainer> compensationContainers = Lists.newArrayList();
        for (String clusterName: compensationClusters.keySet()) {
            String compensateListenerBeanName = KafkaCompensationClusterBeanConfigurer.getCompensateListenerBeanName(clusterName);
            try {
                ConcurrentMessageListenerContainer bean = SpringContextUtil.getBean(compensateListenerBeanName, ConcurrentMessageListenerContainer.class);
                compensationContainers.add(bean);
            } catch (Exception e) {
                log.error("<kafka-tool> Kafka 获取补偿监听container:{} 异常:{}", compensateListenerBeanName, ExceptionUtils.getStackTrace(e));
            }
        }
        return compensationContainers;
    }

    public static Map<String,String> convertHeaderMap(Headers headers){
        if (headers == null){
            return null;
        }
        Map<String,String> headerMap = Maps.newHashMap();
        headers.forEach(header -> headerMap.put(header.key(), new String(header.value())));
        return headerMap;
    }


    public static Method getExecuteMethod(Class cla, String name) {
        Method[] methods = cla.getDeclaredMethods();
        for (Method m : methods) {
            if (name.equals(m.getName())) {
                return m;
            }
        }
        return null;
    }

    public static boolean isConsumerRecord(Method method) {
        for (int i = 0; i < method.getParameterCount(); i++) {
            MethodParameter methodParameter = new MethodParameter(method, i);
            Type parameterType = methodParameter.getGenericParameterType();
            if (parameterIsType(parameterType, ConsumerRecord.class)) {
                return true;
            }

        }
        return false;
    }

    public static boolean isConsumerRecordList(Method method) {
        for (int i = 0; i < method.getParameterCount(); i++) {
            MethodParameter methodParameter = new MethodParameter(method, i);
            if (isConsumerRecordList(methodParameter)) {
                return true;
            }

        }
        return false;
    }

    public static int getConsumerRecordParamIndex(Method method){
        int targetParameterIndex = -1;
        for (int i = 0; i < method.getParameterCount(); i++) {
            MethodParameter methodParameter = new MethodParameter(method, i);
            Type parameterType = methodParameter.getGenericParameterType();
            boolean isConsumerRecord = parameterIsType(parameterType, ConsumerRecord.class);
            boolean isConsumerRecordList = isConsumerRecordList(methodParameter);

            if (isConsumerRecord || isConsumerRecordList){
                targetParameterIndex = i;
                break;
            }
        }
        return targetParameterIndex;
    }

    /** @see MessagingMessageListenerAdapter parameterIsType(Type, Type) */
    private static boolean parameterIsType(Type parameterType, Type type) {
        if (parameterType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) parameterType;
            Type rawType = parameterizedType.getRawType();
            if (rawType.equals(type)) {
                return true;
            }
        }
        return parameterType.equals(type);
    }

    private static boolean isConsumerRecordList(MethodParameter methodParameter) {
        Type genericParameterType = methodParameter.getGenericParameterType();
        if (genericParameterType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) genericParameterType;
            if (parameterizedType.getRawType().equals(List.class) && parameterizedType.getActualTypeArguments().length == 1) {
                Type paramType = parameterizedType.getActualTypeArguments()[0];
                boolean isConsumerRecordList =	paramType.equals(ConsumerRecord.class)
                        || (paramType instanceof ParameterizedType
                        && ((ParameterizedType) paramType).getRawType().equals(ConsumerRecord.class)
                        || (paramType instanceof WildcardType
                        && ((WildcardType) paramType).getUpperBounds() != null
                        && ((WildcardType) paramType).getUpperBounds().length > 0
                        && ((WildcardType) paramType).getUpperBounds()[0] instanceof ParameterizedType
                        && ((ParameterizedType) ((WildcardType)
                        paramType).getUpperBounds()[0]).getRawType().equals(ConsumerRecord.class))
                );
                return isConsumerRecordList;
            }
        }
        return false;
    }
}

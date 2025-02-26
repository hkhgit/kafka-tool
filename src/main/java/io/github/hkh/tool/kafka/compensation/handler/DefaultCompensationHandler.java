package io.github.hkh.tool.kafka.compensation.handler;

import com.google.common.collect.Lists;
import io.github.hkh.tool.kafka.compensation.CompensationResult;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.kafka.CompensationMockAck;
import io.github.hkh.tool.kafka.utils.KafkaConsumerUtil;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.lang.reflect.Method;

/**
 * Description: 默认补偿处理器
 *
 * @author kai
 * @date 2025/1/15
 */
@Slf4j
public class DefaultCompensationHandler implements CompensationHandler {

    @Override
    public CompensationResult doSingleCompensation(CompensationMessage record) {
        try {
            ConsumerRecord<String, String> consumerRecord =
                    new ConsumerRecord<>(record.getRecordTopic(), record.getRecordPartition(), record.getRecordOffset(), record.getRecordKey(), record.getRecordValue());
            if (MapUtils.isNotEmpty(record.getRecordHeaders())) {
                record.getRecordHeaders().forEach((k, v) -> consumerRecord.headers().add(k, v.getBytes()));
            }
            // 业务方法uri格式为:类名.方法名 通过反射调用业务方法
            Class<?> businessClass = Class.forName(record.getOriginalMethod().substring(0, record.getOriginalMethod().lastIndexOf(".")));
            String methodName = record.getOriginalMethod().substring(record.getOriginalMethod().lastIndexOf(".") + 1);
            Method executeMethod = KafkaConsumerUtil.getExecuteMethod(businessClass, methodName);
            if (executeMethod == null){
                log.error("<kafka-tool>自动补偿尝试执行原listener方法获取为null, method:{}", record.getOriginalMethod());
                return new CompensationResult(false);
            }
            //构造参数
            Class<?>[] parameterTypes = executeMethod.getParameterTypes();
            if (parameterTypes.length > 2){
                log.error("<kafka-tool>自动补偿尝试执行原listener方法获取 参数 > 2, method:{},", record.getOriginalMethod());
                return new CompensationResult(false);
            }
            Object[] params = new Object[parameterTypes.length];
            int consumerRecordParamIndex = KafkaConsumerUtil.getConsumerRecordParamIndex(executeMethod);
            boolean isConsumerRecord = KafkaConsumerUtil.isConsumerRecord(executeMethod);
            boolean isConsumerRecordList = KafkaConsumerUtil.isConsumerRecordList(executeMethod);
            if (isConsumerRecord){
                params[consumerRecordParamIndex] = consumerRecord;
            }else if (isConsumerRecordList){
                params[consumerRecordParamIndex] = Lists.newArrayList(consumerRecord);
            }
            if (parameterTypes.length == 2 && parameterTypes[1 - consumerRecordParamIndex].equals(Acknowledgment.class)){
                //如果参数为2，则第二个参数为Acknowledgment
                Acknowledgment ack = new CompensationMockAck();
                params[1 - consumerRecordParamIndex] = ack;
            }
            Object bean = SpringContextUtil.getBean(businessClass);
            executeMethod.invoke(bean, params);
            return new CompensationResult(true);
        }catch (Exception e){
            log.error("<kafka-tool>自动补偿原listener方法失败, method:{}", record.getOriginalMethod(), e.getCause());
            return new CompensationResult(false);
        }
    }



}

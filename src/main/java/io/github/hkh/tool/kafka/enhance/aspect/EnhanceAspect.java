package io.github.hkh.tool.kafka.enhance.aspect;

import com.google.common.collect.Lists;
import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.constant.CompensationStatusEnum;
import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;
import io.github.hkh.tool.kafka.compensation.kafka.KafkaCompensationClusterBeanConfigurer;
import io.github.hkh.tool.kafka.compensation.kafka.KafkaCompensationProducer;
import io.github.hkh.tool.kafka.config.KafkaToolContext;
import io.github.hkh.tool.kafka.enhance.AbstractEnhancedExecutor;
import io.github.hkh.tool.kafka.enhance.EnhanceTask;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.handler.CompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.handler.CompensationHandler;
import io.github.hkh.tool.kafka.compensation.handler.DefaultCompensationHandler;
import io.github.hkh.tool.kafka.compensation.service.PreserveCompensationMessageService;
import io.github.hkh.tool.kafka.config.KafkaToolProperties;
import io.github.hkh.tool.kafka.enhance.EnhanceKafkaListener;
import io.github.hkh.tool.kafka.utils.KafkaConsumerUtil;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Description: 增强切面
 *
 * @author kai
 * @date 2025/1/16
 */
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE + 100)
@Aspect
public class EnhanceAspect extends AbstractEnhancedExecutor {

    KafkaToolProperties kafkaToolProperties;

    @Autowired
    public EnhanceAspect(PreserveCompensationMessageService preserveCompensationMessageService, KafkaToolProperties kafkaToolProperties) {
        super(preserveCompensationMessageService);
        this.kafkaToolProperties = kafkaToolProperties;
    }

    @Pointcut(value = "@annotation(kafkaListenerAnnotation) && @annotation(enhancedKafkaListenerAnnotation)", argNames = "kafkaListenerAnnotation,enhancedKafkaListenerAnnotation")
    private void kafkaListenerEnhancedMethod(KafkaListener kafkaListenerAnnotation, EnhanceKafkaListener enhancedKafkaListenerAnnotation) {}


    @Around(value = "kafkaListenerEnhancedMethod(kafkaListenerAnnotation,enhancedKafkaListenerAnnotation)", argNames = "joinPoint,kafkaListenerAnnotation,enhancedKafkaListenerAnnotation")
    public Object enhanceKafkaListenersWithList(ProceedingJoinPoint joinPoint, KafkaListener kafkaListenerAnnotation, EnhanceKafkaListener enhancedKafkaListenerAnnotation) throws Throwable {
        // 判断是否为重试线程
        if (KafkaToolContext.getCompensatingFlag()) {
            return joinPoint.proceed();
        }
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        // 仅支持List<ConsumerRecord>、ConsumerRecord 参数
        boolean isConsumerRecord = KafkaConsumerUtil.isConsumerRecord(method);
        boolean isConsumerRecordList = KafkaConsumerUtil.isConsumerRecordList(method);
        int targetParameterIndex = KafkaConsumerUtil.getConsumerRecordParamIndex(method);

        if (targetParameterIndex == -1) {
            log.warn("<kafka-tool>未匹配参数List<ConsumerRecord>、ConsumerRecord，不进行增强执行");
            return joinPoint.proceed();
        }
        //找出消息参数，提交线程池处理
        List<Object> consumerRecordList ;
        if (isConsumerRecord){
            consumerRecordList = Lists.newArrayList(args[targetParameterIndex]);
        }else {
            consumerRecordList = (List<Object>)args[targetParameterIndex];
        }
        List<EnhanceTask> enhancedTasks = new ArrayList<>();
        try {
            enhancedTasks = buildEnhanceTask(joinPoint, kafkaListenerAnnotation, enhancedKafkaListenerAnnotation, args, targetParameterIndex, consumerRecordList, isConsumerRecordList);
        }catch (Exception e){
            log.error("<kafka-tool>构建增强任务异常",e);
            joinPoint.proceed();
        }
        if (enhancedTasks.isEmpty()) {
            log.warn("<kafka-tool>enhancedTasks is empty 线程池不执行");
            return joinPoint.proceed();
        }
        int executeCount = executeEnhance(kafkaListenerAnnotation,enhancedTasks);
        if (executeCount != consumerRecordList.size()) {
            log.error("<kafka-tool>本次增强消息处理{}条,实际执行{}条",consumerRecordList.size(),executeCount);
        }
        return null;
    }

    private List<EnhanceTask> buildEnhanceTask(ProceedingJoinPoint joinPoint, KafkaListener kafkaListenerAnnotation, EnhanceKafkaListener enhancedKafkaListenerAnnotation,
                                                 Object[] args, int targetParameterIndex, List<Object> consumerRecordList, boolean isConsumerRecordList) {
        //构造任务队列,创建存储任务的容器
        List<EnhanceTask> tasks = new ArrayList<>();
        for (Object record : consumerRecordList) {
            Object[] allotArgs ;
            if (isConsumerRecordList){
                List allotList = Lists.newArrayList(record);
                allotArgs = args.clone();
                allotArgs[targetParameterIndex] = allotList;
            }else {
                allotArgs = args;
            }
            EnhanceTask aspectEnhancedTask = new AspectEnhanceTask()
                    .setTargetRecord(record)
                    .setTargetParameterIndex(targetParameterIndex)
                    .setJoinPoint(joinPoint)
                    .setAllotArgs(allotArgs)
                    .setKafkaListenerAnnotation(kafkaListenerAnnotation)
                    .setEnhancedKafkaListenerAnnotation(enhancedKafkaListenerAnnotation);
            tasks.add(aspectEnhancedTask);
        }
        return tasks;
    }

    @Data
    @Accessors(chain = true)
    private class AspectEnhanceTask implements EnhanceTask {
        /**
         * 消息
         */
        Object targetRecord;

        int targetParameterIndex;

        /**
         * AOP切点
         */
        ProceedingJoinPoint joinPoint;

        /**
         * 分配的参数
         */
        Object[] allotArgs;

        //KafKaListener注解对象
        KafkaListener kafkaListenerAnnotation;

        //EnhancedKafkaListener注解对象
        EnhanceKafkaListener enhancedKafkaListenerAnnotation;


        @Override
        public String getEnhanceThreadPoolName() {
            return enhancedKafkaListenerAnnotation.threadPoolName();
        }


        @Override
        public void execute() throws Throwable {
            joinPoint.proceed(allotArgs);
        }

        @Override
        public void afterCompensateExecute(boolean isSingleThread){
            //单条消息消费失败后，如果是单线程并且用户手动提交ack时需要提交上去
            if (isSingleThread && allotArgs.length == 2 && allotArgs[1 - targetParameterIndex] instanceof Acknowledgment) {
                ((Acknowledgment)allotArgs[1 - targetParameterIndex]).acknowledge();
            }
        }

        @Override
        public CompensationContext buildCompensationContext() {
            String kafkaCluster = enhancedKafkaListenerAnnotation.kafkaCluster();
            CompensationContext compensateContext = new CompensationContext();
            compensateContext.setCompensationMessage(buildCompensationMessage());
            compensateContext.setCompensateType(enhancedKafkaListenerAnnotation.compensationType());
            compensateContext.setCompensationProperties(kafkaToolProperties.getCompensate());
            compensateContext.setCompensateLimitMax(enhancedKafkaListenerAnnotation.compensateLimitMax());
            Class<? extends CompensationErrorHandler> compensationErrorHandler = enhancedKafkaListenerAnnotation.compensationErrorHandlerClass();
            if (null != compensationErrorHandler){
                CompensationErrorHandler errorHandler = SpringContextUtil.getBean(compensationErrorHandler);
                compensateContext.setCompensationErrorHandler(errorHandler);
            }
            if (CompensationTypeEnum.Kafka.equals(compensateContext.getCompensateType())) {
                compensateContext.setKafkaCluster(kafkaCluster);
                compensateContext.setKafkaTypeCompensationProperties(kafkaToolProperties.getCompensate().getClusters().get(kafkaCluster));
                // 根据kafkaCluster配置动态注入producer
                String compensateProducerBeanName = KafkaCompensationClusterBeanConfigurer.getDynamicCompensateProducerBeanName(kafkaCluster);
                KafkaCompensationProducer compensationProducer = SpringContextUtil.getBean(compensateProducerBeanName, KafkaCompensationProducer.class);
                compensateContext.setKafkaCompensationProducer(compensationProducer);
            }
            return compensateContext;
        }



        @Override
        public CompensationMessage buildCompensationMessage() {
            CompensationMessage compensationMessage = new CompensationMessage();
            Method originalMethod = ((MethodSignature) joinPoint.getSignature()).getMethod();
            String originalMethodStr = originalMethod.getDeclaringClass().getName() + "." + originalMethod.getName();
            compensationMessage.setOriginalMethod(originalMethodStr);

            //设置重试相关:默认为重试原方法，否则为获取实现CompensationHandler的方法
            Class<? extends CompensationHandler> compensationHandlerClass = enhancedKafkaListenerAnnotation.compensationHandlerClass();
            if(compensationHandlerClass.equals(DefaultCompensationHandler.class)){
                compensationMessage.setCompensateOriginalMethod(true);
            }else {
                String[] compensations = SpringContextUtil.getApplicationContext().getBeanNamesForType(compensationHandlerClass);
                compensationMessage.setCompensationHandler(compensations[0]);
            }
            Class<? extends CompensationErrorHandler> errorHandlerClass = enhancedKafkaListenerAnnotation.compensationErrorHandlerClass();
            if (null!= errorHandlerClass){
                String[] errorHandlers = SpringContextUtil.getApplicationContext().getBeanNamesForType(errorHandlerClass);
                compensationMessage.setCompensationErrorHandler(errorHandlers[0]);
            }
            //设置补偿初始状态相关
            compensationMessage.setCompensationCluster(enhancedKafkaListenerAnnotation.kafkaCluster());
            compensationMessage.setCompensationStatus(CompensationStatusEnum.UNFINISHED.getStatus());
            compensationMessage.setCompensationInterval(enhancedKafkaListenerAnnotation.compensateInterval());
            compensationMessage.setCompensationCount(0);
            compensationMessage.setCompensationLimitMax(enhancedKafkaListenerAnnotation.compensateLimitMax());
            //设置消息
            if (targetRecord instanceof ConsumerRecord){
                ConsumerRecord errorMsgRecord = (ConsumerRecord) targetRecord;
                Map<String, String> headerMap = KafkaConsumerUtil.convertHeaderMap(errorMsgRecord.headers());
                String topic = errorMsgRecord.topic();
                Object key = errorMsgRecord.key();
                Object value = errorMsgRecord.value();
                long offset = errorMsgRecord.offset();
                int partition = errorMsgRecord.partition();
                long timestamp = errorMsgRecord.timestamp();
                compensationMessage.setRecordKey(String.valueOf(key))
                        .setRecordValue(String.valueOf(value))
                        .setRecordHeaders(headerMap)
                        .setRecordOffset(offset)
                        .setRecordPartition(partition)
                        .setRecordTopic(topic)
                        .setRecordTimestamp(timestamp);
            } else {
                log.warn("<kafka-tool>消息类型{}不支持", targetRecord.getClass().getDeclaringClass().getName());
            }
            return compensationMessage;
        }


    }

}

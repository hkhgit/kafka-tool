package io.github.hkh.tool.kafka.enhance;

import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;
import io.github.hkh.tool.kafka.compensation.handler.DefaultCompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.handler.DefaultCompensationHandler;
import io.github.hkh.tool.kafka.compensation.handler.CompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.handler.CompensationHandler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EnhanceKafkaListener {

    /**
     * 处理线程池名称，可传自定义线程池 的bean name
     */
    String threadPoolName() default "";

    /**
     * 补偿类型
     */
    CompensationTypeEnum compensationType();

    /**
     * kafka 补偿集群名称
     */
    String kafkaCluster() default "";

    /**
     * 自动补偿最大重试次数
     */
    int compensateLimitMax() default 5;

    /**
     * 自动补偿间隔时间
     */
    int compensateInterval() default 300;

    /**
     * 自定义重试处理方法 , 需实现CompensationHandler的class并且为spring bean
     */
    Class<? extends CompensationHandler> compensationHandlerClass() default DefaultCompensationHandler.class;

    /**
     * 自定义重试失败处理方法, 需实现CompensationErrorHandler的class并且为spring bean
     */
    Class<? extends CompensationErrorHandler> compensationErrorHandlerClass() default DefaultCompensationErrorHandler.class;

}

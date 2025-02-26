package io.github.hkh.tool.kafka.enhance;

import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;

/**
 * Description: 增强任务
 *
 * @author kai
 * @date 2025/1/16
 */
public interface EnhanceTask {

    /**
     * 执行任务
     *
     * @throws Throwable 异常
     */
    void execute() throws Throwable;

    /**
     * 获取线程池
     *
     * @return 线程池
     */
    String getEnhanceThreadPoolName();

    /**
     * 当增强消费任务出现补偿后的处理
     * @param isSingleThread 是否为单线程
     */
    default void afterCompensateExecute(boolean isSingleThread){

    }

    /**
     * 实现重试上下文
     *
     * @return 重试上下文
     */
    CompensationContext buildCompensationContext();

    /**
     * 消费记录
     *
     * @return 消费记录
     */
    CompensationMessage buildCompensationMessage();

}

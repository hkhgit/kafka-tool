package io.github.hkh.tool.kafka.compensation.strategy;

import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;

/**
 * Description: 重试策略
 *
 * @author kai
 * @date 2025/1/17
 */
public interface CompensationStrategy {


    /**
     * 重试类型
     *
     * @return 重试类型
     */
    CompensationTypeEnum getCompensationType();


    /**
     * 补偿执行
     *
     * @param proceedException 异常
     * @return 执行结果
     */
    CompensationStrategyResult execute(Throwable proceedException, CompensationContext compensationContext);
}

package io.github.hkh.tool.kafka.compensation.strategy;

import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.CompensationResult;
import io.github.hkh.tool.kafka.compensation.Compensator;
import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * Description: 本地补偿
 *
 * @author kai
 * @date 2025/1/17
 */
@Slf4j
public class LocalCompensationStrategy implements CompensationStrategy{

    @Override
    public CompensationTypeEnum getCompensationType() {
        return CompensationTypeEnum.Local;
    }

    @Override
    public CompensationStrategyResult execute(Throwable proceedException, CompensationContext compensationContext) {
        int compensateLimitMax = compensationContext.getCompensateLimitMax();
        for (int i = 0; i < compensateLimitMax; i++) {
            CompensationResult compensationResult = Compensator.compensate(compensationContext.getCompensationMessage());
            if (compensationResult.isCompensateSuccess()) {
                return new CompensationStrategyResult(true);
            }
            log.error("<kafka-tool>本地补偿第{}次重试失败", compensationContext.getCompensationMessage().getCompensationCount());
        }
        return new CompensationStrategyResult(true);
    }
}

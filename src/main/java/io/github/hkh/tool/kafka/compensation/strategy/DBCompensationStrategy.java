package io.github.hkh.tool.kafka.compensation.strategy;

import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.constant.CompensationTypeEnum;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.mapper.CompensationMessageMapper;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/17
 */
@Slf4j
public class DBCompensationStrategy implements CompensationStrategy{


    @Override
    public CompensationTypeEnum getCompensationType() {
        return CompensationTypeEnum.DB;
    }

    @Override
    public CompensationStrategyResult execute(Throwable proceedException, CompensationContext compensationContext) {
        try {
            CompensationMessage compensationMessage = compensationContext.getCompensationMessage();
            // 保存到数据库
            CompensationMessageMapper compensationMessageMapper = SpringContextUtil.getBean(CompensationMessageMapper.class);
            compensationMessageMapper.insert(compensationMessage);
            return new CompensationStrategyResult(true);
        }catch (Exception e){
            log.error("<kafka-tool>数据库补偿保存消息失败",e);
            return new CompensationStrategyResult(false);
        }
    }
}

package io.github.hkh.tool.kafka.compensation.handler;

import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.service.PreserveCompensationMessageService;
import lombok.extern.slf4j.Slf4j;

/**
 * Description: 补偿失败默认保存至数据库
 *
 * @author kai
 * @date 2025/1/24
 */
@Slf4j
public class DefaultCompensationErrorHandler implements CompensationErrorHandler{

    private final PreserveCompensationMessageService preserveCompensationMessageService;

    public DefaultCompensationErrorHandler(PreserveCompensationMessageService preserveCompensationMessageService) {
        this.preserveCompensationMessageService = preserveCompensationMessageService;
    }

    @Override
    public void afterErrorCompensation(CompensationMessage compensationMessage) {
        log.info("<kafka-tool>补偿失败，保存至数据库, topic: {}, partition:{}, offset: {}", compensationMessage.getRecordTopic(), compensationMessage.getRecordPartition(), compensationMessage.getRecordOffset());
        preserveCompensationMessageService.saveOrUpdate(compensationMessage);
    }
}


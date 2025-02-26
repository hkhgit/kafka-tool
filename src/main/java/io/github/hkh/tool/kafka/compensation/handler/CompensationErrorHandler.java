package io.github.hkh.tool.kafka.compensation.handler;

import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/15
 */
public interface CompensationErrorHandler {

    void afterErrorCompensation(CompensationMessage record);

}

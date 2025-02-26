package io.github.hkh.tool.kafka.compensation.handler;

import io.github.hkh.tool.kafka.compensation.CompensationResult;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/15
 */
public interface CompensationHandler {

    CompensationResult doSingleCompensation(CompensationMessage record);

}

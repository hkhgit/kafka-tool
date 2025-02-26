package io.github.hkh.tool.kafka.compensation;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/17
 */
@Data
@AllArgsConstructor
public class CompensationResult {

    boolean compensateSuccess;

    boolean arriveErrorLimit;

    public CompensationResult(boolean exeSuccess) {
        this.compensateSuccess = exeSuccess;
    }


}

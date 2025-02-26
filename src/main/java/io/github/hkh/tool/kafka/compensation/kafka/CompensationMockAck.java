package io.github.hkh.tool.kafka.compensation.kafka;

import org.springframework.kafka.support.Acknowledgment;

/**
 * Description: 补偿模拟ack
 *
 * @author kai
 * @date 2025/1/15
 */
public class CompensationMockAck implements Acknowledgment {
    @Override
    public void acknowledge() {
        // 模拟手动ack
    }
}

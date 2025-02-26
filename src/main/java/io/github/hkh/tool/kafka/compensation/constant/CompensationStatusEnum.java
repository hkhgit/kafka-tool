package io.github.hkh.tool.kafka.compensation.constant;

/**
 * Description: 补偿消息状态枚举
 *
 * @author kai
 * @date 2025/2/4
 */
public enum CompensationStatusEnum {
    /**
     * 未完成
     */
    UNFINISHED("UNFINISHED", "未完成"),

    /**
     * 重试中
     */
    RETRYING("RETRYING", "重试中"),

    /**
     * 挂起
     */
    HANGUP("HANGUP", "挂起"),

    /**
     * 已完成
     */
    FINISHED("FINISHED", "已完成");

    private final String status;

    private final String description;

    CompensationStatusEnum(String status, String description) {
        this.status = status;
        this.description = description;
    }

    public String getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

}

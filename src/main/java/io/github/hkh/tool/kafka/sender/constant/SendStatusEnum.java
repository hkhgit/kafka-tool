package io.github.hkh.tool.kafka.sender.constant;

import lombok.Getter;

/**
 * Description: 发送状态枚举
 *
 * @author kai
 * @date 2025/1/8
 */
public enum SendStatusEnum {
    WAITING(0, "待处理"),

    SENT(1, "已处理"),

    MAX_RETRIES(2, "已达最大重试次数"),
    ;

    @Getter
    private final Integer status;

    @Getter
    private final String description;

    SendStatusEnum(Integer status, String description) {
        this.status = status;
        this.description = description;
    }

}

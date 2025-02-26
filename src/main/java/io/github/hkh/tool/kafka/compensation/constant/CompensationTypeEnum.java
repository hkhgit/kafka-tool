package io.github.hkh.tool.kafka.compensation.constant;

/**
 * Description: 补偿类型枚举
 *
 * @author kai
 * @date 2025/1/3
 */
public enum CompensationTypeEnum {

    /**
     * 内存重试
     */
    Local("local", "内存重试"),

    /**
     * 数据库重试
     */
    DB("db", "数据库重试"),

    /**
     * Kafka重试
     */
    Kafka("kafka", "Kafka重试"),
    ;



    private final String type;

    private final String description;

    CompensationTypeEnum(String type, String description) {
        this.type = type;
        this.description = description;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public static CompensationTypeEnum getFromType(String type) {
        for (CompensationTypeEnum value : values()) {
            if (value.getType().equals(type)) {
                return value;
            }
        }
        return null;
    }

}

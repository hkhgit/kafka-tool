package io.github.hkh.tool.kafka.config.compensation;

import lombok.Data;

/**
 * Description: kafka补偿配置类
 *
 * @author kai
 * @date 2025/1/3
 */
@Data
public class KafkaTypeCompensationProperties {

    /**
     * 补偿队列kafka集群地址
     */
    private String bootstrapServers;

    /**
     * 补偿队列交换机
     */
    private String topic;

    /**
     * 补偿主题消费组名称
     */
    private String groupId;

//    /**
//     * 自动补偿唤醒定时任务间隔时间(毫秒)
//     */
//    public int schedulerIntervalMs = 300000;
//
//
//    /**
//     * 一次poll的消息数
//     */
//    private final int maxPollRecords = 1;

    /**
     * poll的最大间隔时间
     */
    private final int maxPollIntervalMs = 300000;

}

package io.github.hkh.tool.kafka.config.compensation;

import lombok.Data;

import java.util.Map;

/**
 * Description: 补偿配置类
 *
 * @author kai
 * @date 2025/1/3
 */
@Data
public class CompensationProperties {

    /**
     * 是否启用自动补偿
     */
    public Boolean enabled;

    /**
     * 每次补偿的最大条数
     */
    private int schedulerCompensationMaxLimit = 50;

    /**
     * 自动补偿唤醒定时任务间隔时间(毫)
     */
    public int schedulerInterval = 300;

    /**
     * kafka补偿集群配置
     */
    private Map<String, KafkaTypeCompensationProperties> clusters;
}

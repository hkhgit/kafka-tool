package io.github.hkh.tool.kafka.compensation.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.handlers.JacksonTypeHandler;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Description: 补偿消息
 *
 * @author kai
 * @date 2025/1/4
 */
@Data
@TableName("kafka_compensation_message")
@Accessors(chain = true)
public class CompensationMessage {

    @TableId(type = IdType.AUTO)
    private Long id;

    @TableField(value = "record_headers",typeHandler = JacksonTypeHandler.class)
    private Map<String, String> recordHeaders;

    @TableField("record_key")
    private String recordKey;

    @TableField("record_value")
    private String recordValue;

    @TableField("record_partition")
    private Integer recordPartition;

    @TableField("record_offset")
    private Long recordOffset;

    @TableField("record_topic")
    private String recordTopic;

    @TableField("record_timestamp")
    private Long recordTimestamp;

    @TableField("original_method")
    private String originalMethod;

    @TableField("is_original_method")
    private boolean compensateOriginalMethod;

    @TableField("compensation_handler")
    private String compensationHandler;

    @TableField("compensation_error_handler")
    private String compensationErrorHandler;

    @TableField("compensation_cluster")
    private String compensationCluster;


    @TableField("compensation_status")
    private String compensationStatus;

    @TableField("compensation_count")
    private Integer compensationCount;

    @TableField("compensation_limit_max")
    private Integer compensationLimitMax;

    @TableField("compensation_interval")
    private Integer compensationInterval;

    @TableField("last_compensation_time")
    private LocalDateTime lastCompensateTime;

    @TableField("next_compensation_time")
    private LocalDateTime nextCompensateTime;

    /**
     *创建时间
     */
    @TableField("create_time")
    private LocalDateTime createTime;

    /**
     *更新时间
     */
    @TableField("last_update_time")
    private LocalDateTime lastUpdateTime;

    /**
     *是否删除
     */
    @TableField("is_delete")
    private Boolean isDelete;
}

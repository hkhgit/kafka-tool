package io.github.hkh.tool.kafka.sender.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.handlers.JacksonTypeHandler;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Description: 发送异常消息
 *
 * @author kai
 * @date 2025/1/8
 */
@Data
@TableName("kafka_send_error_msg")
public class SendExceptionMsg implements Serializable {

    /**
     *serialVersionUID
     */
    private static final long serialVersionUID = 1L;

    /**
     *主键
     */
    @TableId(type = IdType.AUTO)
    private Long id;

    /**
     *消息序号
     */
    @TableField("trace_id")
    private String traceId;

    /**
     *目的主题名
     */
    @TableField("topic_name")
    private String topicName;

    /**
     *连接模板名
     */
    @TableField("template_name")
    private String templateName;

    /**
     * headers
     */
    @TableField(value = "record_headers",typeHandler = JacksonTypeHandler.class)
    private String recordHeaders;

    /**
     *key名
     */
    @TableField("record_key")
    private String recordKey;

    /**
     *消息体
     */
    @TableField("record_value")
    private String recordValue;



    /**
     *状态：0 待处理，1 已处理
     */
    @TableField("send_status")
    private Integer sendStatus;

    /**
     *重试次数记录
     */
    @TableField("retries")
    private Integer retries;

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

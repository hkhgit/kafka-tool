package io.github.hkh.tool.kafka.sender.service;

import io.github.hkh.tool.kafka.sender.entity.SendExceptionMsg;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/8
 */
public interface PreserveSendMsgService {

    /**
     * 保存消息
     *
     * @param traceId    消息序列号
     * @param templateName 模板名称
     * @param record       消息
     */
    void preserveMessage(String traceId, String templateName, ProducerRecord<String, String> record);

    /**
     * 获取待处理消息（默认重试次数 5）
     *
     * @param limit limit
     * @return list
     */
    List<SendExceptionMsg> queryUnSolvedMessage(int limit);

    /**
     * 标记完成
     *
     * @param id 主键
     */
    void markCompleted(Long id);
}

package io.github.hkh.tool.kafka.sender.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import io.github.hkh.tool.kafka.utils.JsonUtil;
import io.github.hkh.tool.kafka.utils.KafkaConsumerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Resource;
import io.github.hkh.tool.kafka.sender.constant.ReliableSenderConstant;
import io.github.hkh.tool.kafka.sender.constant.SendStatusEnum;
import io.github.hkh.tool.kafka.sender.entity.SendExceptionMsg;
import io.github.hkh.tool.kafka.sender.mapper.SendMsgMapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Description: 默认消息保存服务实现
 *
 * @author kai
 * @date 2025/1/8
 */
@Slf4j
public class DefaultPreserveSendMsgServiceImpl implements PreserveSendMsgService{

    @Resource
    private SendMsgMapper sendMsgMapper;

    @Override
    public void preserveMessage(String traceId, String templateName, ProducerRecord<String, String> record) {
        SendExceptionMsg sendMsgRecord = new SendExceptionMsg();
        sendMsgRecord.setTraceId(traceId);
        sendMsgRecord.setTemplateName(templateName);
        sendMsgRecord.setTopicName(record.topic());
        sendMsgRecord.setRecordKey(record.key());
        sendMsgRecord.setRecordValue(record.value());
        // 先检查消息头里是否存在 id，如果存在，更新重试次数即可
        Header header = record.headers().lastHeader(ReliableSenderConstant.RELIABLE_SEND_RECORD_ID_HEADER);
        if (Objects.nonNull(header)) {
            long id = Long.parseLong(new String(header.value()));
            SendExceptionMsg data = sendMsgMapper.selectById(id);
            if (Objects.nonNull(data)) {
                data.setRetries(data.getRetries() + 1);
                // 发送失败重置状态
                data.setSendStatus(SendStatusEnum.WAITING.getStatus());
                // 达到重试次数，日志告警
                if (data.getRetries() >= ReliableSenderConstant.RELIABLE_SEND_RECORD_RETRIES) {
                    data.setSendStatus(SendStatusEnum.MAX_RETRIES.getStatus());
                    log.error("<kafka-tool>Kafka消息弹性发送失败，达到重试次数 {}， id: {}，消息将不会再重试", data.getRetries(), id);
                }
                sendMsgMapper.updateById(data);
            }
        }else {
            Map<String, String> headerMap = KafkaConsumerUtil.convertHeaderMap(record.headers());
            sendMsgRecord.setRecordHeaders(JsonUtil.bean2Json(headerMap));
            sendMsgMapper.insert(sendMsgRecord);
        }
    }

    @Override
    public List<SendExceptionMsg> queryUnSolvedMessage(int limit) {
        QueryWrapper<SendExceptionMsg> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda()
                .eq(SendExceptionMsg::getSendStatus, SendStatusEnum.WAITING.getStatus())
                .le(SendExceptionMsg::getRetries, ReliableSenderConstant.RELIABLE_SEND_RECORD_RETRIES)
                .eq(SendExceptionMsg::getIsDelete, false)
                .orderByDesc(SendExceptionMsg::getId)
                .last("limit " + limit);

        return sendMsgMapper.selectList(queryWrapper);
    }

    @Override
    public void markCompleted(Long id) {
        LambdaUpdateWrapper<SendExceptionMsg> lambdaUpdateWrapper = new LambdaUpdateWrapper<>();
        lambdaUpdateWrapper
                .set(SendExceptionMsg :: getSendStatus, SendStatusEnum.SENT.getStatus())
                .eq(SendExceptionMsg::getId, id)
                .eq(SendExceptionMsg :: getSendStatus, SendStatusEnum.WAITING.getStatus())
                .eq(SendExceptionMsg::getIsDelete, false);
        SendExceptionMsg record = new SendExceptionMsg();
        record.setSendStatus(SendStatusEnum.SENT.getStatus());

        sendMsgMapper.update(lambdaUpdateWrapper);
    }
}

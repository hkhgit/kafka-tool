package io.github.hkh.tool.kafka.sender;

import io.github.hkh.tool.kafka.sender.service.PreserveSendMsgService;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: 可靠发送者
 *
 * @author kai
 * @date 2025/1/8
 */
// TODO: 2025/1/8 可靠发送需要与traceId关联起来 
@Slf4j
public class ReliableSender {

    private PreserveSendMsgService preserveSendMsgService;

    private static final Map<String, KafkaTemplate<String, String>> CACHED_TEMPLATE = new ConcurrentHashMap<>();

    public ReliableSender(PreserveSendMsgService preserveSendMsgService) {
        this.preserveSendMsgService = preserveSendMsgService;
    }

    private KafkaTemplate<String, String> getCachedTemplate(String templateName) {
        return CACHED_TEMPLATE.computeIfAbsent(templateName, SpringContextUtil::getBean);
    }

    /**
     * 弹性发送消息
     *
     * @param templateName KafkaTemplate名称
     * @param topic        topic
     * @param key          key
     * @param message      消息
     * @param headers      消息头
     */
    public void send(String templateName, String topic, String key, String message, Map<String,String> headers) {
        if (StringUtils.isBlank(key)) {
            // key 为空字符串时，替换为 null，防止分区消息不均匀
            key = null;
        }
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
        if (MapUtils.isNotEmpty(headers)) {
            headers.forEach((k, v) -> producerRecord.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        }
        send(getCachedTemplate(templateName), templateName, producerRecord);
    }

    public void send(KafkaTemplate<String, String> template, ProducerRecord<String, String> producerRecord) {
        String[] kafkaTemplateNames = SpringContextUtil.getApplicationContext().getBeanNamesForType(template.getClass());
        send(template, kafkaTemplateNames[0], producerRecord);
    }

    public void send(KafkaTemplate<String, String> template, String templateName, ProducerRecord<String, String> record) {
        //关联id，追踪一次发送过程
        String traceId = UUID.randomUUID().toString().replaceAll("-", StringUtils.EMPTY);
        log.info("<kafka-tool>[{}] 可靠发送 send msg to kafka topic:{}, templateName:{}, key:{}", traceId, record.topic(), templateName, record.key());
        try {
            template.send(record).addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    // do nothing
                }

                @Override
                public void onFailure(Throwable ex) {
                    log.warn("<kafka-tool> 可靠发送 send msg:[{}] to kafka fail: {}", traceId, ex);
                    try {
                        preserveSendMsgService.preserveMessage(traceId, templateName, record);
                    } catch (Exception e) {
                        log.error("<kafka-tool> [{}] Kafka弹性发送, Topic: {}, 异常回调时尝试保存消息异常:{}", traceId, record.topic(), record.value(), e);
                    }
                }
            });
        } catch (Exception ex) {
            try {
                preserveSendMsgService.preserveMessage(traceId, templateName, record);
            } catch (Exception e) {
                log.error("<kafka-tool> [{}] Kafka弹性发送, Topic: {}, 异常回调时尝试保存消息异常:{}", traceId, record.topic(), record.value(), e);
                throw e;
            }
        }
    }
}

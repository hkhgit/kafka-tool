package io.github.hkh.tool.kafka.compensation.scheduler;

import io.github.hkh.tool.kafka.concurrency.AdjustContext;
import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import io.github.hkh.tool.kafka.config.compensation.KafkaTypeCompensationProperties;
import io.github.hkh.tool.kafka.utils.KafkaConsumerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Description: kafka补偿唤醒调度器
 *
 * @author kai
 * @date 2025/1/4
 */
@Slf4j
public class KafkaCompensationWakeUpScheduler implements CommandLineRunner {

    private static final ScheduledExecutorService TIMER = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("kafka-auto-compensate-scheduler");
        thread.setDaemon(true);
        return thread;
    });

    private final KafkaTypeCompensationProperties kafkaTypeCompensationProperties;

    private final String cluster;

    private final CompensationProperties compensationProperties;


    public KafkaCompensationWakeUpScheduler(CompensationProperties compensationProperties,String cluster) {
        this.cluster = cluster;
        this.compensationProperties = compensationProperties;
        this.kafkaTypeCompensationProperties = compensationProperties.getClusters().get(cluster);
    }

    @Override
    public void run(String... args) throws Exception {

        int schedulerInterval = compensationProperties.getSchedulerInterval();
        String compensationTopic = kafkaTypeCompensationProperties.getTopic();
        String compensationGroupId = kafkaTypeCompensationProperties.getGroupId();
        TIMER.scheduleAtFixedRate(() -> {
            //检查补偿topic是否也已经被暂停
            if (checkCompensationTopicPaused(compensationTopic, compensationGroupId)){
                log.info("<kafka-tool>kafka补偿唤醒调度器, 补偿topic:{} 已经被暂停, 不需要恢复监听器容器", compensationTopic);
                return;
            }
            Collection<ConcurrentMessageListenerContainer> compensationListenerContainers = KafkaConsumerUtil.getCompensationListenerContainers();
            if (CollectionUtils.isNotEmpty(compensationListenerContainers)) {
                compensationListenerContainers.forEach(container -> {
                    try {
                        String[] topics = container.getContainerProperties().getTopics();
                        if (Objects.nonNull(topics) && topics.length == 1 && topics[0].equals(compensationTopic)) {
                            if (container.isPauseRequested()) {
                                container.resume();
                                log.info("<kafka-tool>kafka补偿唤醒调度器, 恢复补偿topic:{} 监听器容器成功", compensationTopic);
                            }else if (!container.isRunning()) {
                                container.start();
                                log.info("<kafka-tool>kafka补偿唤醒调度器, 启动补偿topic:{} 监听器容器成功", compensationTopic);
                            }
                        }
                    } catch (Exception e) {
                        log.error("<kafka-tool>kafka补偿唤醒调度器, 恢复补偿topic:{} 监听器容器异常, 异常信息: {}", compensationTopic, ExceptionUtils.getStackTrace(e));
                    }
                });
            }
        }, 5, schedulerInterval, TimeUnit.SECONDS);
        log.info("<kafka-tool>Kafka自动补偿, KafkaCompensationWakeUpScheduler 启动成功, schedulerInterval: {}, topic: {}", schedulerInterval, compensationTopic);
    }

    private boolean checkCompensationTopicPaused(String topic,String groupId) {
        Integer adjustMatchConcurrency = AdjustContext.getTopicConcurrency(topic, groupId);
        return adjustMatchConcurrency != null && adjustMatchConcurrency <= 0;
    }
}

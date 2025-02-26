package io.github.hkh.tool.kafka.compensation.scheduler;

import io.github.hkh.tool.kafka.compensation.CompensationResult;
import io.github.hkh.tool.kafka.compensation.Compensator;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.handler.CompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.handler.DefaultCompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.service.PreserveCompensationMessageService;
import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.boot.CommandLineRunner;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Description: DB补偿任务
 *
 * @author kai
 * @date 2025/2/4
 */
@Slf4j
public class DBCompensationScheduler implements CommandLineRunner {

    private static final ScheduledExecutorService TIMER = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("db-auto-compensate-scheduler");
        thread.setDaemon(true);
        return thread;
    });

    PreserveCompensationMessageService preserveCompensationMessageService;

    CompensationProperties compensationProperties;

    public DBCompensationScheduler(PreserveCompensationMessageService preserveCompensationMessageService, CompensationProperties compensationProperties) {
        this.compensationProperties = compensationProperties;
        this.preserveCompensationMessageService = preserveCompensationMessageService;
    }

    @Override
    public void run(String... args) throws Exception {
        //消息补偿间隔
        int schedulerInterval = compensationProperties.getSchedulerInterval();
        int compensationMaxLimit = compensationProperties.getSchedulerCompensationMaxLimit();
        TIMER.scheduleWithFixedDelay(() -> {
            try {
                //查询数据库中状态为待补偿的消息
                List<CompensationMessage> compensationMessageList = preserveCompensationMessageService.getUnFinishCompensationMessageList(compensationMaxLimit);
                if (CollectionUtils.isEmpty(compensationMessageList)) {
                    return;
                }
                //执行补偿
                compensationMessageList.forEach(compensationMessage -> {
                    boolean lock = preserveCompensationMessageService.lockAndDealMessage(compensationMessage);
                    if (lock) {
                        CompensationResult compensationResult = Compensator.compensate(compensationMessage);
                        //如果补偿失败已经满足重试次数并且是默认补偿失败处理器处理，则不需要重复保存数据库
                        CompensationErrorHandler compensationErrorHandler = SpringContextUtil.getBean(compensationMessage.getCompensationErrorHandler(), CompensationErrorHandler.class);
                        if (!(compensationResult.isArriveErrorLimit() && compensationErrorHandler.getClass().equals(DefaultCompensationErrorHandler.class))) {
                            preserveCompensationMessageService.updateMessageStatusAndCount(compensationMessage);
                        }
                    }
                });
            }catch (Exception e){
                log.error("<kafka-tool>DB自动补偿, 本次调度异常", e);
            }
        }, 5, schedulerInterval, TimeUnit.SECONDS);
        log.info("<kafka-tool>DB自动补偿, DBCompensationScheduler 启动成功, schedulerInterval: {}", schedulerInterval);
    }

}

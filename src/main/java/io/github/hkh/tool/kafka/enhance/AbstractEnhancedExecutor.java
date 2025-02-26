package io.github.hkh.tool.kafka.enhance;

import io.github.hkh.tool.kafka.compensation.CompensationContext;
import io.github.hkh.tool.kafka.compensation.constant.CompensationStatusEnum;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.handler.CompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.service.PreserveCompensationMessageService;
import io.github.hkh.tool.kafka.compensation.strategy.CompensationStrategy;
import io.github.hkh.tool.kafka.compensation.strategy.CompensationStrategyFactory;
import io.github.hkh.tool.kafka.compensation.strategy.CompensationStrategyResult;
import io.github.hkh.tool.kafka.config.KafkaToolContext;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/17
 */
@Slf4j
public abstract class AbstractEnhancedExecutor {

    protected PreserveCompensationMessageService preserveCompensationMessageService;

    public AbstractEnhancedExecutor(PreserveCompensationMessageService preserveCompensationMessageService) {
        this.preserveCompensationMessageService = preserveCompensationMessageService;
    }

    protected int executeEnhance(KafkaListener kafkaListenerAnnotation, List<? extends EnhanceTask> tasks) {
        long startTime = System.currentTimeMillis();
        //获取线程池
        ExecutorService enhanceThreadPool = getEnhanceThreadPool(tasks);
        if (enhanceThreadPool == null) {
            log.info("<kafka-tool>获取增强线程池为空,本次消费不使用线程池");
            for (EnhanceTask task : tasks) {
                executeCompensateTask(task,true);
            }
            return tasks.size();
        }

        // 构造任务列表
        List<Callable<Long>> callableTasks = new ArrayList<>();
        for (EnhanceTask task : tasks) {
            callableTasks.add(() -> executeCompensateTask(task,false));
        }

        //获取超时时间
        int enhanceOutTime = getEnhanceOutTime(kafkaListenerAnnotation);

        List<Future<Long>> results = new ArrayList<>();
        try {
            results = enhanceThreadPool.invokeAll(callableTasks, enhanceOutTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("<kafka-tool>线程池执行异常", e);
            checkExeThreadPoolHealth(enhanceThreadPool);
            tasks.forEach(task -> executeCompensateError(task.buildCompensationContext(), e));
        }

        checkExeThreadPoolHealth(enhanceThreadPool);

        int successCount = 0;
        int errorCount = 0;
        for (int i = 0; i < results.size(); i++) {
            Future<Long> result = results.get(i);
            boolean done = result.isDone();
            boolean cancelled = result.isCancelled();
            if (!done || cancelled) {
                errorCount++;
                log.warn("<kafka-tool>线程池执行超时，执行自定义异常回调");
                executeCompensateError(tasks.get(i).buildCompensationContext(), new RuntimeException("<kafka-tool>线程池执行超时"));
            } else {
                successCount++;
            }
        }
        log.info("<kafka-tool>本次增强消息处理{}条完毕,耗时{}ms",tasks.size(),System.currentTimeMillis() - startTime);
        return successCount + errorCount;
    }

    /**
     * 线程池执行超时时间
     *
     * @return 超时时间
     */
    private int getEnhanceOutTime(KafkaListener kafkaListenerAnnotation){
        try{
            ConcurrentKafkaListenerContainerFactory containerFactory = SpringContextUtil.getBean(kafkaListenerAnnotation.containerFactory(), ConcurrentKafkaListenerContainerFactory.class);
            return containerFactory.getContainerProperties().getMonitorInterval();
        }catch (Exception e){
            log.warn("<kafka-tool>获取线程池超时时间错误，默认60s", e);
        }
        return 60;
    }

    protected long executeCompensateTask(EnhanceTask task,boolean isSingleThread) {
        long startTime = System.currentTimeMillis();
        try {
            task.execute();
        } catch (Throwable e) {
            log.error("<kafka-tool>task执行异常", e);
            //根据策略配置初始化重试策略并重试
            CompensationContext compensationContext = task.buildCompensationContext();
            CompensationMessage compensationMessage = compensationContext.getCompensationMessage();
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime nextCompensateTime = now.plusSeconds(compensationMessage.getCompensationInterval());
            compensationMessage.setNextCompensateTime(nextCompensateTime);
            CompensationStrategy compensationStrategy = CompensationStrategyFactory.getStrategy(compensationContext);
            CompensationStrategyResult strategyResult = compensationStrategy.execute(e, compensationContext);
            //重试失败，调用自定义异常处理
            if (!strategyResult.isStrategyDone()) {
                executeCompensateError(compensationContext, e);
            }
            task.afterCompensateExecute(isSingleThread);
        }
        return System.currentTimeMillis() - startTime;
    }

    /**
     * 补偿失败，调用自定义异常处理
     *
     */
    protected void executeCompensateError(CompensationContext compensationContext, Throwable exeError) {
        CompensationMessage compensationMessage = compensationContext.getCompensationMessage();
        //补偿失败时补偿状态为挂起
        compensationMessage.setCompensationStatus(CompensationStatusEnum.HANGUP.getStatus());
        if (null == compensationContext.getCompensationErrorHandler()) {
            //当无自定义处理时，默认保存到数据库
            preserveCompensationMessageService.save(compensationMessage);
        }else{
            int partition = compensationMessage.getRecordPartition();
            Long offset = compensationMessage.getRecordOffset();
            log.warn("<kafka-tool>消费消息失败,调用自定义异常消息,partition:{},offset:{}", partition, offset);
            try {
                CompensationErrorHandler compensationErrorHandler = compensationContext.getCompensationErrorHandler();
                CompensationErrorHandler bean = SpringContextUtil.getBean(compensationErrorHandler.getClass());
                bean.afterErrorCompensation(compensationMessage);
            }catch (Exception e){
                log.error("<kafka-tool>消费消息失败,调用自定义异常消息处理失败", e);
            }
        }
    }

    /**
     * 获取增强线程池
     *
     * @param tasks 任务列表
     * @return 增强线程池
     */
    private ExecutorService getEnhanceThreadPool(List<? extends EnhanceTask> tasks) {
        String enhanceThreadPoolName = tasks.get(0).getEnhanceThreadPoolName();
        if (StringUtils.isBlank(enhanceThreadPoolName)) {
            return null;
        }
        return KafkaToolContext.getThreadPoolExecutorMap().computeIfAbsent(enhanceThreadPoolName, k -> {
            Object customThreadPool = SpringContextUtil.getBean(enhanceThreadPoolName);
            if (customThreadPool instanceof ExecutorService) {
                return (ExecutorService) customThreadPool;
            }else {
                return null;
            }
        });
    }

    private void checkExeThreadPoolHealth(ExecutorService exeThreadPool) {
        if (exeThreadPool.isTerminated() || exeThreadPool.isTerminated() || exeThreadPool.isShutdown()) {
            log.info("<kafka-tool>线程池已关闭，线程池不执行");
            try {
                sleep(60000);
            } catch (InterruptedException ee) {
                log.warn("<kafka-tool>线程池已关闭，挂起失败", ee);
            }
            throw new RuntimeException("<kafka-tool>线程池已关闭");
        }
    }















}

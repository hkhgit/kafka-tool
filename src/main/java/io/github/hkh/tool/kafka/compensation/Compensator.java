package io.github.hkh.tool.kafka.compensation;

import io.github.hkh.tool.kafka.compensation.constant.CompensationStatusEnum;
import io.github.hkh.tool.kafka.compensation.constant.DefaultConstant;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.handler.CompensationErrorHandler;
import io.github.hkh.tool.kafka.compensation.handler.CompensationHandler;
import io.github.hkh.tool.kafka.compensation.handler.DefaultCompensationHandler;
import io.github.hkh.tool.kafka.config.KafkaToolContext;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.BeansException;

import java.time.LocalDateTime;

/**
 * Description:
 *
 * @author kai
 * @date 2025/2/6
 */
@Slf4j
public class Compensator {

    public static CompensationResult compensate(CompensationMessage compensationMessage) {
        boolean compensateOriginalMethod = compensationMessage.isCompensateOriginalMethod();
        CompensationHandler compensationHandler;
        //补偿回去原入口方法
        if (compensateOriginalMethod){
            compensationHandler = new DefaultCompensationHandler();
        }else {
            try {
                compensationHandler = SpringContextUtil.getBean(compensationMessage.getCompensationHandler(), CompensationHandler.class);
            }catch (BeansException e){
                log.info("<kafka-tool>自动补偿尝试从spring容器中获取CompensationHandler实例失败, name:{}", compensationMessage.getCompensationHandler());
                return new CompensationResult(false, false);
            }
        }
        KafkaToolContext.setCompensatingFlag();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextCompensateTime = now.plusSeconds(compensationMessage.getCompensationInterval());
        compensationMessage.setLastCompensateTime(now);
        compensationMessage.setNextCompensateTime(nextCompensateTime);
        compensationMessage.setCompensationCount(compensationMessage.getCompensationCount() + 1);
        //执行补偿
        CompensationResult compensationResult = compensationHandler.doSingleCompensation(compensationMessage);
        if (compensationResult.isCompensateSuccess()){
            compensationMessage.setCompensationStatus(CompensationStatusEnum.FINISHED.getStatus());
        }else {
            if (compensationMessage.getCompensationLimitMax() != DefaultConstant.RETRY_LIMIT_MAX && compensationMessage.getCompensationCount() >= compensationMessage.getCompensationLimitMax()) {
                log.info("<kafka-tool>自动补偿失败, 已到重试次数上限: {}次! 业务方法uri:{}, 消息:{}", compensationMessage.getCompensationLimitMax(), compensationMessage.getOriginalMethod(), compensationMessage.getRecordValue());
                compensationMessage.setCompensationStatus(CompensationStatusEnum.HANGUP.getStatus());
                compensationResult.setArriveErrorLimit(true);
                //补偿失败已经满足重试次数, 执行补偿失败处理
                if (StringUtils.isNotBlank(compensationMessage.getCompensationErrorHandler())){
                    try {
                        CompensationErrorHandler compensationErrorHandler = SpringContextUtil.getBean(compensationMessage.getCompensationErrorHandler(), CompensationErrorHandler.class);
                        compensationErrorHandler.afterErrorCompensation(compensationMessage);
                    } catch (BeansException e) {
                        log.error("<kafka-tool>自动补偿失败, 补偿处理失败, 获取不到补偿失败处理类:{}", compensationMessage.getCompensationErrorHandler());
                    } catch (Exception e) {
                        log.error("<kafka-tool>自动补偿失败, 补偿处理失败, 异常:{}", ExceptionUtils.getStackTrace(e));
                    }
                }
            }else{
                log.info("<kafka-tool>Kafka自动补偿, 第{}次补偿失败, nextCompensateTime:{}", compensationMessage.getCompensationCount(), compensationMessage.getNextCompensateTime());
                compensationMessage.setCompensationStatus(CompensationStatusEnum.UNFINISHED.getStatus());
            }
        }
        KafkaToolContext.removeCompensatingFlag();
        return compensationResult;
    }
}

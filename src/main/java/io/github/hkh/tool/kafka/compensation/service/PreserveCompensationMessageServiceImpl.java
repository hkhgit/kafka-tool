package io.github.hkh.tool.kafka.compensation.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import io.github.hkh.tool.kafka.compensation.constant.CompensationStatusEnum;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import io.github.hkh.tool.kafka.compensation.mapper.CompensationMessageMapper;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Description:
 *
 * @author kai
 * @date 2025/2/5
 */
@Service
public class PreserveCompensationMessageServiceImpl extends ServiceImpl<CompensationMessageMapper, CompensationMessage> implements PreserveCompensationMessageService{


    @Override
    public List<CompensationMessage> getUnFinishCompensationMessageList(int limit) {
//        LocalDateTime now = LocalDateTime.now();
//        String formattedNow = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        QueryWrapper<CompensationMessage> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda()
                .eq(CompensationMessage::getCompensationStatus, CompensationStatusEnum.UNFINISHED.getStatus())
                .le(CompensationMessage::getNextCompensateTime, LocalDateTime.now())
                .orderByAsc(CompensationMessage::getNextCompensateTime)
                .last("limit " + limit);
        return baseMapper.selectList(queryWrapper);
    }

    @Override
    public boolean lockAndDealMessage(CompensationMessage compensationMessage) {
        return this.lambdaUpdate()
                .eq(CompensationMessage :: getCompensationCluster, compensationMessage.getCompensationCluster())
                .eq(CompensationMessage ::getRecordTopic, compensationMessage.getRecordTopic())
                .eq(CompensationMessage ::getRecordPartition, compensationMessage.getRecordPartition())
                .eq(CompensationMessage ::getRecordOffset, compensationMessage.getRecordOffset())
                .set(CompensationMessage :: getCompensationStatus, CompensationStatusEnum.RETRYING.getStatus())
                .update();
    }

    @Override
    public void updateMessageStatusAndCount(CompensationMessage compensationMessage) {
        this.lambdaUpdate()
                .eq(CompensationMessage :: getCompensationCluster, compensationMessage.getCompensationCluster())
                .eq(CompensationMessage ::getRecordTopic, compensationMessage.getRecordTopic())
                .eq(CompensationMessage ::getRecordPartition, compensationMessage.getRecordPartition())
                .eq(CompensationMessage ::getRecordOffset, compensationMessage.getRecordOffset())
                .set(CompensationMessage :: getCompensationStatus, compensationMessage.getCompensationStatus())
                .set(CompensationMessage ::getCompensationCount, compensationMessage.getCompensationCount())
                .set(CompensationMessage ::getLastCompensateTime, compensationMessage.getLastCompensateTime())
                .update();
    }


}

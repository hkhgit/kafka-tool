package io.github.hkh.tool.kafka.compensation.service;

import com.baomidou.mybatisplus.extension.service.IService;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;

import java.util.List;

/**
 * Description: 补偿消息持久化服务
 *
 * @author kai
 * @date 2025/2/5
 */
public interface PreserveCompensationMessageService extends IService<CompensationMessage> {

    /**
     * Description: 获取待补偿消息列表
     *
     * @author kai
     * @date  2025/2/5
     * @param limit
     * @return List<CompensationMessage>
     **/
    List<CompensationMessage> getUnFinishCompensationMessageList(int limit);

    /**
     * Description:  锁定待补偿消息
     *
     * @author kai
     * @date  2025/2/5
     * @param compensationMessage
     * @return boolean
     **/
    boolean lockAndDealMessage(CompensationMessage compensationMessage);


    /**
     * Description:  更新补偿消息状态与次数
     *
     * @author kai
     * @date  2025/2/5
     * @param compensationMessage
     **/
    void updateMessageStatusAndCount(CompensationMessage compensationMessage);

}

package io.github.hkh.tool.kafka.sender.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

import io.github.hkh.tool.kafka.sender.entity.SendExceptionMsg;

/**
 * Description:
 *
 * @author kai
 * @date 2025/1/9
 */
@Mapper
public interface SendMsgMapper extends BaseMapper<SendExceptionMsg> {
}

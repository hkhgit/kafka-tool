package io.github.hkh.tool.kafka.compensation.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.hkh.tool.kafka.compensation.entity.CompensationMessage;
import org.apache.ibatis.annotations.Mapper;

/**
 * Description:
 *
 * @author kai
 * @date 2025/2/4
 */
@Mapper
public interface CompensationMessageMapper extends BaseMapper<CompensationMessage> {
}

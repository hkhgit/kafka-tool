package io.github.hkh.tool.kafka.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Description: jackson工具类
 *
 * @author kai
 * @date 2025/1/6
 */
public class JsonUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        // 设置时区
        mapper.setTimeZone(TimeZone.getDefault());
        // 日期类型格式
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        // 序列化所有参数，包括null
        mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        mapper.registerModule(new JavaTimeModule());
    }

    public static String bean2Json(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T> T json2Bean(String jsonStr, Class<T> objClass) {
        try {
            return mapper.readValue(jsonStr, objClass);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}

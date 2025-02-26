package io.github.hkh.tool.kafka.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Description: kafka-tool上下文
 *
 * @author kai
 * @date 2025/1/3
 */
public class KafkaToolContext {

    /**
     * Description: 是否初始化检查
     */
    private static boolean isInitCheck = false;


    private static final ThreadLocal<Boolean> compensatingFlag = new ThreadLocal<>();

    /**
     * Description: 增强线程池map
     */
    private static final Map<String, ExecutorService> threadPoolExecutorMap = new ConcurrentHashMap<>();


    public boolean isInitCheck() {
        return isInitCheck;
    }

    public static void setInitCheck(boolean initCheck) {
        isInitCheck = initCheck;
    }

    public static Map<String, ExecutorService> getThreadPoolExecutorMap() {
        return threadPoolExecutorMap;
    }

    public static boolean getCompensatingFlag() {
        Boolean flag = compensatingFlag.get();
        if (flag == null) {
            return false;
        }
        return flag;
    }

    public static void setCompensatingFlag() {
        compensatingFlag.set(Boolean.TRUE);
    }

    public static void removeCompensatingFlag() {
        compensatingFlag.remove();
    }

}

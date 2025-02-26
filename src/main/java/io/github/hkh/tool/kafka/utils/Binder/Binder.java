package io.github.hkh.tool.kafka.utils.Binder;

import org.springframework.core.env.Environment;

/**
 * Description: Binder接口
 *
 * @author kai
 * @date 2025/1/3
 */
public interface Binder {

    /**
     * 给绑定目标对象绑定值
     *
     * @param targetClass 目标类
     * @param prefix      前缀
     * @param <T>         返回值类型
     * @return 返回绑定好值的目标对象
     */
    <T> T bind(Class<T> targetClass, String prefix);

    /**
     * 初始化environment和执行反射准备工作
     *
     * @param env environment
     */
    void init(Environment env);

}

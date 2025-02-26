package io.github.hkh.tool.kafka.utils.Binder;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.core.env.Environment;

import java.lang.reflect.Method;

/**
 * Description: SpringBoot2版本Binder
 *
 * @author kai
 * @date 2025/1/3
 */
@Slf4j
public class SpringBoot2Binder implements Binder{

    private static Method bindMethod;
    private static Object bind;

    @Override
    public <T> T bind(Class<T> targetClass, String prefix) {
        try {
            Object bindResult = bindMethod.invoke(bind, prefix, targetClass);
            if (!((BindResult) bindResult).isBound()) {
                return targetClass.newInstance();
            }
            Method resultGetMethod = bindResult.getClass().getDeclaredMethod("get");
            return (T) resultGetMethod.invoke(bindResult);
        } catch (Exception e) {
            log.warn("<kafka-tool>bind配置失败", e);
        }
        return null;
    }

    @Override
    public void init(Environment env) {
        try {
            Class<?> bindClass = Class.forName("org.springframework.boot.context.properties.bind.Binder");
            Method getMethod = bindClass.getDeclaredMethod("get", Environment.class);
            bindMethod = bindClass.getDeclaredMethod("bind", String.class, Class.class);
            bind = getMethod.invoke(null, env);
        } catch (Exception e) {
            log.warn("<kafka-tool>反射准备bind信息失败", e);
        }
    }
}

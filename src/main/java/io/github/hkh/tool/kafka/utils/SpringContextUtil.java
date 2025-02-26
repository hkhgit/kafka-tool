package io.github.hkh.tool.kafka.utils;

import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

/**
 * Description: spring上下文工具类
 *
 * @author kai
 * @date 2024/12/30
 */
@Lazy(false)
@ConditionalOnMissingBean(SpringContextUtil.class)
public class SpringContextUtil implements ApplicationContextAware, EnvironmentAware {

    private static ApplicationContext applicationContext;

    private static Environment environment;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        System.out.println("SpringContextUtil applicationContext:" + applicationContext);
        SpringContextUtil.applicationContext = applicationContext;
    }

    @Override
    public void setEnvironment(Environment environment) {
        System.out.println("SpringContextUtil environment:" + environment);
        SpringContextUtil.environment = environment;
    }

    public static String getProperty(String key){
        return environment.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        return environment.getProperty(key, defaultValue);
    }

    public static <T> T getBean(String name) {
        return (T) getApplicationContext().getBean(name);
    }

    public static <T> T getBean(Class<T> clazz) {
        return getApplicationContext().getBean(clazz);
    }

    public static <T> T getBean(String name, Class<T> clazz) {
        return getApplicationContext().getBean(name, clazz);
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public static Environment getEnvironment() {
        return environment;
    }
}

package io.github.hkh.tool.kafka.utils.Binder;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Description: Binder工具类
 *
 * @author kai
 * @date 2025/1/3
 */
@Slf4j
public class BinderUtil {

    public static final Binder BIND;
    private static final AtomicBoolean INITED = new AtomicBoolean(false);

    static {
        Binder bind = null;
        try {
            //boot2
            Class.forName("org.springframework.boot.context.properties.bind.Binder");
            bind = new SpringBoot2Binder();
        } catch (Exception e) {
            //boot1
            log.error("<kafka-tool> 不支持的版本，请检查");
        }
        BIND = bind;
    }

    public static void initBinder(Environment env) {
        if (!INITED.compareAndSet(false, true)) {
            return;
        }
        BIND.init(env);
    }

    public static <T> T bind(Class<T> targetClass, String prefix) {
        return BIND.bind(targetClass, prefix);
    }

}

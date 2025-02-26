package io.github.hkh.tool.kafka.concurrency;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * kafka业务标识
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaBizTag {

    /**
     * 业务标识
     */
    String name();
}

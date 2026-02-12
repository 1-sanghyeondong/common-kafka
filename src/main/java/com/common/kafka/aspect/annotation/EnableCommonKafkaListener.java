package com.common.kafka.aspect.annotation;

import com.common.kafka.configuration.KafkaPlatformAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({ KafkaPlatformAutoConfiguration.class })
public @interface EnableCommonKafkaListener {
}
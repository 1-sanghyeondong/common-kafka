package com.common.kafka.configuration;

import com.common.kafka.aspect.KafkaResiliencyAspect;
import com.common.kafka.resource.PlatformKafkaProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@AutoConfiguration
@EnableConfigurationProperties(PlatformKafkaProperties.class)
public class KafkaPlatformAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public KafkaResiliencyAspect kafkaResiliencyAspect(KafkaTemplate<String, Object> kafkaTemplate, PlatformKafkaProperties properties) {
        return new KafkaResiliencyAspect(kafkaTemplate, properties);
    }
}
package com.common.kafka.configuration;

import com.common.kafka.aspect.KafkaResiliencyAspect;
import com.common.kafka.aspect.annotation.EnableCommonKafkaListener;
import com.common.kafka.resource.PlatformKafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@ConditionalOnBean(annotation = EnableCommonKafkaListener.class)
@EnableConfigurationProperties({PlatformKafkaProperties.class})
public class KafkaPlatformAutoConfiguration {
    private final String bootstrapServers;

    public KafkaPlatformAutoConfiguration(
            /**
             * 기입하지 않을 경우 에러 반환 / 부트업 실패
             */
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        this.bootstrapServers = bootstrapServers;
    }

    private Map<String, Object> getDefaultProducerProps() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 50000);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return properties;
    }

    private Map<String, Object> getDefaultConsumerProps() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        return properties;
    }

    @Bean("commonProducerFactory")
    public ProducerFactory<String, Object> commonProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> properties = new HashMap<>(getDefaultProducerProps());
        properties.putAll(kafkaProperties.buildProducerProperties(null));
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean("commonKafkaTemplate")
    public KafkaTemplate<String, Object> commonKafkaTemplate(
            @Qualifier("commonProducerFactory") ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean("commonConsumerFactory")
    public ConsumerFactory<String, Object> commonConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> properties = new HashMap<>(getDefaultConsumerProps());
        properties.putAll(kafkaProperties.buildConsumerProperties(null));
        properties.putIfAbsent(JsonDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean("commonContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> commonContainerFactory(
            @Qualifier("commonConsumerFactory") ConsumerFactory<String, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 0L));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    @Bean("commonStringConsumerFactory")
    public ConsumerFactory<String, String> commonStringConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> properties = new HashMap<>(getDefaultConsumerProps());
        properties.putAll(kafkaProperties.buildConsumerProperties(null));

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        properties.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        properties.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean("commonStringContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> commonStringContainerFactory(
            @Qualifier("commonStringConsumerFactory") ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 0L));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaResiliencyAspect kafkaResiliencyAspect(
            @Qualifier("commonKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            PlatformKafkaProperties properties) {
        return new KafkaResiliencyAspect(kafkaTemplate, properties);
    }
}
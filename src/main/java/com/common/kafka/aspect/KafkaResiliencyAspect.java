package com.common.kafka.aspect;

import com.common.kafka.constant.ResiliencyHeader;
import com.common.kafka.resource.PlatformKafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Aspect
@Component
public class KafkaResiliencyAspect {

    private static final Logger log = LoggerFactory.getLogger(KafkaResiliencyAspect.class);
    private static final String BLOCK_KEY_SEPARATOR = ":";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PlatformKafkaProperties properties;
    private final Map<String, Long> localBlockCache = new ConcurrentHashMap<>();

    public KafkaResiliencyAspect(KafkaTemplate<String, Object> kafkaTemplate, PlatformKafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Around("@annotation(com.common.kafka.aspect.annotation.CommonKafkaListener)")
    public Object handleResiliency(ProceedingJoinPoint joinPoint) throws Throwable {
        ConsumerRecord<?, ?> record = extractConsumerRecord(joinPoint);

        if (record == null || record.key() == null) {
            return joinPoint.proceed();
        }

        String key = record.key().toString();
        String sourceTopic = record.topic();
        String blockKey = sourceTopic + BLOCK_KEY_SEPARATOR + key;

        if (isBlocked(blockKey)) {
            log.info("key[{}] is blocked (local cache). forwarding to retry topic", key);
            forwardToRetryTopic(record, sourceTopic, key, null);
            return null; // ACK
        }

        try {
            return joinPoint.proceed();
        } catch (Exception e) {
            log.error("key[{}] processing failed. forwarding to retry topic | message: {}", key, e.getMessage(), e);

            try {
                long ttlMillis = properties.getBlockingTtlSeconds() * 1000L;
                localBlockCache.put(blockKey, System.currentTimeMillis() + ttlMillis);

                forwardToRetryTopic(record, sourceTopic, key, e);
                return null;

            } catch (Exception sendEx) {
                log.error("failed to forward to retry topic. rethrowing...", sendEx);
                localBlockCache.remove(blockKey);
                throw e;
            }
        }
    }

    private boolean isBlocked(String blockKey) {
        Long expireAt = localBlockCache.get(blockKey);
        if (expireAt == null) {
            return false;
        }

        if (System.currentTimeMillis() > expireAt) {
            localBlockCache.remove(blockKey);
            return false;
        }
        return true;
    }

    private ConsumerRecord<?, ?> extractConsumerRecord(ProceedingJoinPoint joinPoint) {
        return Arrays.stream(joinPoint.getArgs())
                .filter(arg -> arg instanceof ConsumerRecord)
                .map(arg -> (ConsumerRecord<?, ?>) arg)
                .findFirst()
                .orElse(null);
    }

    private void forwardToRetryTopic(ConsumerRecord<?, ?> record, String sourceTopic, String key, Exception ex) {
        String retryTopic = sourceTopic + properties.getRetryTopicSuffix();
        Object payload = (record.value() != null) ? record.value() : "";

        var messageBuilder = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, retryTopic)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(ResiliencyHeader.ORIGINAL_TOPIC.getKey(), sourceTopic)
                .setHeader(ResiliencyHeader.FORWARDED_AT.getKey(), String.valueOf(System.currentTimeMillis()));

        if (record.headers() != null) {
            for (Header header : record.headers()) {
                messageBuilder.setHeader(header.key(), header.value());
            }
        }

        if (ex != null) {
            messageBuilder.setHeader(ResiliencyHeader.EXCEPTION_MSG.getKey(), ex.getMessage());
            messageBuilder.setHeader(ResiliencyHeader.EXCEPTION_TYPE.getKey(), ex.getClass().getName());
        }

        try {
            CompletableFuture<?> future = kafkaTemplate.send(messageBuilder.build());
            future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("forwarding failed", e);
        }
    }
}
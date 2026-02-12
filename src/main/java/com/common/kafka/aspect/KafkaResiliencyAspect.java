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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * kafka 장애 격리 및 순서 보장을 처리
 * <p>
 * 1. 로직 실패 시 예외를 잡아서 retry 토픽으로 메시지를 이관
 * 2. 실패한 key 를 로컬 캐시에 잠시 저장(blocking)하여 해당 key 의 후속 메시지들도 순서가 뒤바뀌지 않도록 즉시 retry
 * 토픽으로 전송
 */
@Aspect
@Component
public class KafkaResiliencyAspect {

    private static final Logger log = LoggerFactory.getLogger(KafkaResiliencyAspect.class);
    private static final String BLOCK_KEY_SEPARATOR = ":";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PlatformKafkaProperties properties;

    /**
     * 실패한 Key 를 일정 시간 동안 차단하기 위한 인메모리 로컬 캐시
     * key: {topic:message_key}
     * value: {차단 해제 시간(timestamp 형태)}
     */
    private final Map<String, Long> localBlockCache = new ConcurrentHashMap<>();

    public KafkaResiliencyAspect(@Qualifier("commonKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
                                 PlatformKafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Around("@annotation(kafkaListener)")
    public Object handleResiliency(ProceedingJoinPoint joinPoint, com.common.kafka.aspect.annotation.CommonKafkaListener kafkaListener) throws Throwable {

        // enableResiliency 체크 - false 면 resiliency 로직을 완전히 우회
        if (!kafkaListener.enableResiliency()) {
            return joinPoint.proceed();
        }

        ConsumerRecord<?, ?> record = extractConsumerRecord(joinPoint);

        // consumerRecord, key 가 없으면 로직을 적용할 수 없으므로 원본 실행
        if (record == null || record.key() == null) {
            log.warn("record key is not exist");
            return joinPoint.proceed();
        }

        String key = record.key().toString();
        String sourceTopic = record.topic();
        String blockKey = sourceTopic + BLOCK_KEY_SEPARATOR + key;

        if (isBlocked(blockKey)) {
            // 이미 실패해서 차단된 Key 라면
            log.info("key[{}] is blocked (local cache). forwarding to retry topic", key);
            // 비즈니스 로직을 실행하지 않고 skip, 바로 retry 토픽으로 보냄
            forwardToRetryTopic(record, sourceTopic, key, null);
            return null; // 정상 종료(ack) 처리하여 다음 메시지를 받도록 함
        }

        try {
            return joinPoint.proceed();
        } catch (Exception e) {
            // 로직 수행 중 예외 발생 시
            log.error("key[{}] processing failed. forwarding to retry topic | message: {}", key, e.getMessage(), e);

            try {
                // key 차단 설정 (설정된 TTL 만큼)
                long ttlMillis = properties.getBlockingTtlSeconds() * 1000L;
                localBlockCache.put(blockKey, System.currentTimeMillis() + ttlMillis);

                // retry 토픽으로 메시지 이관
                forwardToRetryTopic(record, sourceTopic, key, e);
                return null; // kafka 에게는 성공(ack)으로 보고하여 현재 파티션의 lag 을 방지

            } catch (Exception sendEx) {
                // retry 토픽 전송마저 실패한 경우 (심각한 상황)
                log.error("failed to forward to retry topic. rethrowing...", sendEx);
                // 차단을 풀고 예외를 다시 던져서 kafka 가 재시도하게 만듦 (데이터 유실 방지)
                localBlockCache.remove(blockKey);
                throw e;
            }
        }
    }

    /**
     * 해당 key가 현재 차단(blocking) 상태인지 확인
     * TTL 이 지났다면 차단을 해제
     */
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

    /**
     * 메시지를 retry 토픽으로 전송
     * 원본 헤더를 유지하며 예외 정보를 추가합니다.
     *
     * @param record      원본 메시지
     * @param sourceTopic 원본 토픽명
     * @param key         메시지 키
     * @param ex          발생한 예외 (null 일 수 있음)
     */
    private void forwardToRetryTopic(ConsumerRecord<?, ?> record, String sourceTopic, String key, Exception ex) {
        String retryTopic = properties.getRetryTopic();
        Object payload = (record.value() != null) ? record.value() : "";

        // 메타데이터 헤더 추가
        var messageBuilder = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, retryTopic)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(ResiliencyHeader.ORIGINAL_TOPIC.getKey(), sourceTopic)
                .setHeader(ResiliencyHeader.FORWARDED_AT.getKey(), String.valueOf(System.currentTimeMillis()));

        // 기존 헤더 복사 (trace id 등 유지)
        if (record.headers() != null) {
            for (Header header : record.headers()) {
                messageBuilder.setHeader(header.key(), header.value());
            }
        }

        // 예외 정보가 있다면 헤더에 추가
        if (ex != null) {
            messageBuilder.setHeader(ResiliencyHeader.EXCEPTION_MSG.getKey(), ex.getMessage());
            messageBuilder.setHeader(ResiliencyHeader.EXCEPTION_TYPE.getKey(), ex.getClass().getName());
        }

        try {
            // 동기(sync) 전송으로 확실하게 브로커 저장을 보장 (timeout 3초)
            CompletableFuture<?> future = kafkaTemplate.send(messageBuilder.build());
            future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("forwarding failed", e);
        }
    }

    private ConsumerRecord<?, ?> extractConsumerRecord(ProceedingJoinPoint joinPoint) {
        return Arrays.stream(joinPoint.getArgs())
                .filter(arg -> arg instanceof ConsumerRecord)
                .map(arg -> (ConsumerRecord<?, ?>) arg)
                .findFirst()
                .orElse(null);
    }
}
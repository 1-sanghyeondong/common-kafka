package com.common.kafka.resource;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "platform.kafka")
public class PlatformKafkaProperties {

    /**
     * 공통 Retry 토픽 이름 (기본값: common-retry-topic)
     * 모든 실패 메시지는 원본 토픽과 관계없이 이 토픽으로 전송됨
     * 원본 토픽 정보는 x-original-topic 헤더에 저장
     */
    private String retryTopic = "common-retry-topic";

    /**
     * Redis Blocking TTL (기본값: 10초)
     */
    private long blockingTtlSeconds = 10;

}
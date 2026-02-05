package com.common.kafka.resource;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "platform.kafka")
public class PlatformKafkaProperties {

    /**
     * Retry 토픽 접미사 (기본값: -retry-1m)
     * 별도 프로젝트(Retry Worker)가 이 규칙대로 토픽을 구독해야 함
     */
    private String retryTopicSuffix = "-retry-1m";

    /**
     * Redis Blocking TTL (기본값: 70초)
     * Retry Worker가 처리하고 Block을 풀 때까지의 안전 장치 시간
     */
    private long blockingTtlSeconds = 70;

    public String getRetryTopicSuffix() {
        return retryTopicSuffix;
    }

    public void setRetryTopicSuffix(String retryTopicSuffix) {
        this.retryTopicSuffix = retryTopicSuffix;
    }

    public long getBlockingTtlSeconds() {
        return blockingTtlSeconds;
    }

    public void setBlockingTtlSeconds(long blockingTtlSeconds) {
        this.blockingTtlSeconds = blockingTtlSeconds;
    }

    @Override
    public String toString() {
        return "PlatformKafkaProperties{" +
                "retryTopicSuffix='" + retryTopicSuffix + '\'' +
                ", blockingTtlSeconds=" + blockingTtlSeconds +
                '}';
    }
}
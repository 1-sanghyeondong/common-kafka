package com.common.kafka.constant;

public enum ResiliencyHeader {

    ORIGINAL_TOPIC("x-original-topic"),
    FORWARDED_AT("x-forwarded-at"),
    EXCEPTION_MSG("x-exception-msg"),
    EXCEPTION_TYPE("x-exception-type");

    private final String key;

    ResiliencyHeader(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
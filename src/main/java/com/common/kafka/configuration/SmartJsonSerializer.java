package com.common.kafka.configuration;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class SmartJsonSerializer<T> extends JsonSerializer<T> {
    private final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (data instanceof String) {
            return stringSerializer.serialize(topic, headers, (String) data);
        }

        return super.serialize(topic, headers, data);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data instanceof String) {
            return stringSerializer.serialize(topic, (String) data);
        }
        return super.serialize(topic, data);
    }

    @Override
    public void close() {
        stringSerializer.close();
        super.close();
    }
}

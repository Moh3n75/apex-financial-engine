package com.apex.platform.messaging.consumer;

import java.time.Instant;

public record ConsumerContext(

        String consumerGroup,

        String topic,

        int partition,

        long offset,

        String messageKey,

        Instant receivedAt

) {

    public ConsumerContext {

        if (consumerGroup == null || consumerGroup.isBlank()) {
            throw new IllegalArgumentException(
                    "consumerGroup is required"
            );
        }

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "topic is required"
            );
        }

        if (partition < 0) {
            throw new IllegalArgumentException(
                    "partition cannot be negative"
            );
        }

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "offset cannot be negative"
            );
        }

        if (receivedAt == null) {
            throw new IllegalArgumentException(
                    "receivedAt is required"
            );
        }
    }
}
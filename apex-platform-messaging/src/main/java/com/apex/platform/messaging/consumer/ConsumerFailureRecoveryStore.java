package com.apex.platform.messaging.consumer;

import java.util.UUID;

public interface ConsumerFailureRecoveryStore {

    void markRecovered(
            UUID eventId,
            String consumerGroup
    );
}
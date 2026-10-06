package com.apex.platform.messaging.consumer;

import java.time.Instant;

public interface ConsumerFailureStatistics {

    long countByStatus(
            ConsumerFailureStatus status
    );

    long unresolvedCount();

    Instant oldestUnresolvedFailureTime();
}
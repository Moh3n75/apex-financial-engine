package com.apex.platform.messaging.outbox;


import java.time.Duration;
import java.time.Instant;


public class RetryPolicy {


    public Instant nextRetryTime(
            int retryCount
    ) {

        Duration delay = switch (retryCount) {
            case 0 -> Duration.ofSeconds(30);
            case 1 -> Duration.ofMinutes(5);
            case 2 -> Duration.ofMinutes(30);
            default -> Duration.ofHours(2);
        };


        return Instant.now()
                .plus(delay);
    }
}
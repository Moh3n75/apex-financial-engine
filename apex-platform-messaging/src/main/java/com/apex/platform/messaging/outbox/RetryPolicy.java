package com.apex.platform.messaging.outbox;


import java.time.Duration;
import java.time.Instant;


public class RetryPolicy {


    private static final int MAX_RETRIES = 10;


    public boolean canRetry(
            int retryCount
    ) {

        return retryCount < MAX_RETRIES;

    }



    public Instant nextRetryTime(
            int retryCount
    ) {


        Duration delay = switch (retryCount) {
            case 0 -> Duration.ofSeconds(30);
            case 1 -> Duration.ofMinutes(5);
            case 2 -> Duration.ofMinutes(30);
            case 3 -> Duration.ofHours(2);
            default -> Duration.ofHours(6);
        };


        return Instant.now()
                .plus(delay);

    }

}
package com.apex.platform.messaging.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxStore {


    List<OutboxRecord> findBatch(
            int size
    );

    List<OutboxRecord> claimBatch(
            int size,
            String publisherInstanceId,
            Duration leaseDuration
    );


    boolean markPublished(
            Long id,
            UUID claimToken
    );

    boolean markFailed(
            Long id,
            UUID claimToken,
            String error,
            Instant nextRetryAt
    );


    boolean moveToDeadLetter(
            Long id,
            UUID claimToken,
            String error
    );

}
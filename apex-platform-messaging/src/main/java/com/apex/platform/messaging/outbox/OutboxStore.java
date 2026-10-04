package com.apex.platform.messaging.outbox;

import java.util.List;

public interface OutboxStore {


    List<OutboxRecord> findBatch(
            int size
    );


    void markPublished(
            Long id
    );


    void markFailed(
            Long id,
            String error
    );

}
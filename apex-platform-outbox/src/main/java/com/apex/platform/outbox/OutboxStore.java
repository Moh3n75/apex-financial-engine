package com.apex.platform.outbox;

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
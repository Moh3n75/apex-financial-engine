package com.apex.platform.messaging.outbox;

public interface OutboxPublisher {

    void publishBatch();

}
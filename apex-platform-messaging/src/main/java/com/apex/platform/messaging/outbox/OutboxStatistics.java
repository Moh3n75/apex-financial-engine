package com.apex.platform.messaging.outbox;


import java.time.Instant;


public interface OutboxStatistics {


    long pendingCount();


    Instant oldestUnpublishedEventTime();

}
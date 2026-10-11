package com.apex.platform.messaging.consumer;

public interface ConsumerFailureReplay {

    void replay(
            long failureId
    );
}
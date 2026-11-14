package com.apex.platform.messaging.consumer;

public interface EventMessageDispatcher {

    DispatchResult dispatch(
            String serializedEnvelope,
            ConsumerContext context
    );
}
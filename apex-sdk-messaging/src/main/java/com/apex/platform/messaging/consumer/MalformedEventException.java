package com.apex.platform.messaging.consumer;

public final class MalformedEventException
        extends NonRetryableEventException {

    public MalformedEventException(
            String message
    ) {
        super(message);
    }

    public MalformedEventException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
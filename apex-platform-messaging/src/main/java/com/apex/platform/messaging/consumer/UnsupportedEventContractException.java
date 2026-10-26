package com.apex.platform.messaging.consumer;

/** Safe transport identity only; never includes the raw event or business exception text. */
public final class UnsupportedEventContractException extends NonRetryableEventException {
    public UnsupportedEventContractException(String eventType, Integer version, ConsumerContext context) {
        super("Unsupported event contract type=" + safe(eventType) + " version=" + version
                + " topic=" + context.topic() + " partition=" + context.partition() + " offset=" + context.offset());
    }

    private static String safe(String value) {
        if (value == null) {
            return "unavailable";
        }
        String sanitized = value.replaceAll("[^a-zA-Z0-9._-]", "_");
        return sanitized.substring(0, Math.min(128, sanitized.length()));
    }
}

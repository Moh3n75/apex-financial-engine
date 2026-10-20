package com.apex.platform.messaging.consumer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a public, void instance method on a singleton Spring bean as an event handler.
 * Accepts a concrete, non-generic payload type and optionally the original EventMetadata.
 * Registration is validated after singleton/AOP initialization, before listener startup.
 * Invocation uses the Spring proxy; transaction, Inbox and execution context remain Platform-owned.
 * Parameterized/type-variable payloads and non-invocable proxy methods fail startup.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OnCellEvent {

    String type();

    int version();
}

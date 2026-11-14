package com.apex.platform.events.saga;


import java.time.Instant;
import java.util.UUID;


public interface SagaEvent {


    UUID sagaId();


    String sagaType();


    UUID correlationId();


    Instant occurredAt();

}
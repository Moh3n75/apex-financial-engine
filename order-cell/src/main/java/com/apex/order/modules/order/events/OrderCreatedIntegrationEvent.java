package com.apex.order.modules.order.events;

import java.time.Instant;
import java.util.UUID;

import com.apex.platform.events.IntegrationEvent;

public record OrderCreatedIntegrationEvent(
        UUID orderId, String externalReference, String description, Instant createdAt
) implements IntegrationEvent {}

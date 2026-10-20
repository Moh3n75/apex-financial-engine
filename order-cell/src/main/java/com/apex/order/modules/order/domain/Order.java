package com.apex.order.modules.order.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record Order(UUID id, String externalReference, String description, Instant createdAt) {
    public Order {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(createdAt, "createdAt");
        if (externalReference == null || externalReference.isBlank() || externalReference.length() > 64) {
            throw new IllegalArgumentException("externalReference must contain 1 to 64 characters");
        }
        if (description == null || description.isBlank() || description.length() > 200) {
            throw new IllegalArgumentException("description must contain 1 to 200 characters");
        }
    }

    public static Order create(String externalReference, String description, Instant createdAt) {
        return new Order(UUID.randomUUID(), externalReference, description, createdAt);
    }
}

package com.apex.customer.modules.customer.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Customer aggregate for the reference Cell. No Spring / JDBC / HTTP imports.
 */
public final class Customer {

    private final UUID id;
    private final String externalReference;
    private final String displayName;
    private final CustomerStatus status;
    private final Instant createdAt;

    private Customer(
            UUID id,
            String externalReference,
            String displayName,
            CustomerStatus status,
            Instant createdAt
    ) {
        this.id = Objects.requireNonNull(id, "id");
        this.externalReference = externalReference;
        this.displayName = displayName;
        this.status = Objects.requireNonNull(status, "status");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }

    public static Customer create(String externalReference, String displayName, Instant createdAt) {
        return new Customer(
                UUID.randomUUID(),
                normalizeExternalReference(externalReference),
                normalizeDisplayName(displayName),
                CustomerStatus.ACTIVE,
                createdAt
        );
    }

    public static Customer restore(
            UUID id,
            String externalReference,
            String displayName,
            CustomerStatus status,
            Instant createdAt
    ) {
        return new Customer(
                id,
                normalizeExternalReference(externalReference),
                normalizeDisplayName(displayName),
                status,
                createdAt
        );
    }

    public static String normalizeExternalReference(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("customer.validation.externalReference.required");
        }
        String trimmed = value.trim();
        if (trimmed.length() > 64) {
            throw new IllegalArgumentException("customer.validation.externalReference.required");
        }
        return trimmed;
    }

    public static String normalizeDisplayName(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("customer.validation.displayName.required");
        }
        String trimmed = value.trim();
        if (trimmed.length() > 150) {
            throw new IllegalArgumentException("customer.validation.displayName.required");
        }
        return trimmed;
    }

    public UUID id() {
        return id;
    }

    public String externalReference() {
        return externalReference;
    }

    public String displayName() {
        return displayName;
    }

    public CustomerStatus status() {
        return status;
    }

    public Instant createdAt() {
        return createdAt;
    }
}

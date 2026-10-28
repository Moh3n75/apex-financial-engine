package com.apex.platform.grpc.api;

import java.time.Instant;
import java.util.UUID;

/**
 * Identity fields come from the server's trusted peer verifier, never arbitrary tenant/role
 * headers.
 */
public record RequestContext(
    UUID requestId,
    String idempotencyKey,
    String type,
    int version,
    UUID correlationId,
    UUID causationId,
    String originCell,
    String targetCell,
    String tenant,
    String principal,
    Instant deadline) {}

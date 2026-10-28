package com.apex.platform.grpc.api;

import java.util.function.Supplier;

/**
 * Owning-Cell SPI: claim, business work and serialized result MUST commit on the same
 * DB/transaction. Return only after the outer commit. No in-memory fallback. Fingerprint is
 * canonical request SHA-256; a reused key with any identity/contract/ fingerprint mismatch must
 * fail closed. An in-flight ambiguous result is not a rejection.
 */
public interface TransactionalRequestExecutor {
  byte[] execute(RequestContext context, String fingerprint, Supplier<byte[]> operation);
}

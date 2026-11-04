package com.apex.operations.console.security.m36b;

import java.time.Instant;
import java.util.List;

/** Privacy-safe allow/deny audit. Must never include tokens, cookies, SQL, or raw exception text. */
public interface OpsM36bAccessAudit {
  record Entry(
      Instant at,
      String principalHash,
      String tenantId,
      String environment,
      String owner,
      String kind,
      String scope,
      boolean allowed,
      String reasonCode,
      String correlationId) {}

  void record(Entry entry);

  List<Entry> snapshot();
}

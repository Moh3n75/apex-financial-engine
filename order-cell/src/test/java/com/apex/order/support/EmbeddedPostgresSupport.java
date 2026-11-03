package com.apex.order.support;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/**
 * Shared EmbeddedPostgres builder defaults for disposable fixtures.
 *
 * <p>JDBC's default SSL negotiation can spend the entire zonky {@code PGStartupWait} on a single
 * hung {@code enableSSL} read while postmaster is still recovering. Force {@code sslmode=disable}
 * for loopback fixtures so readiness probes fail-fast with {@code FATAL: starting up} until the
 * server is actually ready — without widening the 10s startup wait.
 */
public final class EmbeddedPostgresSupport {
  private EmbeddedPostgresSupport() {}

  public static EmbeddedPostgres.Builder loopbackBuilder() {
    return EmbeddedPostgres.builder().setConnectConfig("sslmode", "disable");
  }
}

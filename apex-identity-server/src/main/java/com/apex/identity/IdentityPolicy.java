package com.apex.identity;

import java.net.URI;
import java.util.Set;
import java.util.UUID;
import org.springframework.core.env.Environment;

final class IdentityPolicy {
  static void validate(Environment e) {
    URI issuer;
    try {
      issuer = URI.create(e.getRequiredProperty("apex.identity.issuer"));
      UUID.fromString(e.getRequiredProperty("apex.identity.tenant-id"));
    } catch (IllegalArgumentException invalid) {
      throw new IllegalStateException("Valid issuer and tenant UUID required");
    }
    if (issuer.getHost() == null
        || issuer.getUserInfo() != null
        || issuer.getQuery() != null
        || issuer.getFragment() != null
        || !Set.of("", "/").contains(issuer.getPath()))
      throw new IllegalStateException("Stable origin-only issuer required");
    boolean test =
        e.matchesProfiles("isolated-test")
            && e.getProperty("apex.identity.allow-loopback-http", Boolean.class, false)
            && Set.of("127.0.0.1", "localhost").contains(issuer.getHost())
            && "127.0.0.1".equals(e.getProperty("server.address"));
    if (!(test && "http".equals(issuer.getScheme()))
        && !("https".equals(issuer.getScheme())
            && (e.getProperty("server.ssl.enabled", Boolean.class, false)
                || e.getProperty("apex.identity.trusted-https-ingress", Boolean.class, false))))
      throw new IllegalStateException("HTTPS issuer and explicit trusted TLS termination required");
    e.getRequiredProperty("apex.identity.key-file");
    e.getRequiredProperty("apex.identity.active-kid");
    if (!"none".equals(e.getProperty("server.forward-headers-strategy", "none")))
      throw new IllegalStateException("Unverified forwarding assertions are not trusted");
  }
}

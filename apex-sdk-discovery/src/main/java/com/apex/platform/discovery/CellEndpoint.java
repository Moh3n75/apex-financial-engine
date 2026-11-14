package com.apex.platform.discovery;

import java.util.Locale;
import java.util.Objects;

/** One ready replica. HTTP and gRPC ports are never interchangeable. */
public record CellEndpoint(
    String serviceId,
    String instanceId,
    String host,
    int httpPort,
    int grpcPort,
    boolean https,
    String tlsAuthority) {
  public CellEndpoint {
    serviceId = logicalName(serviceId);
    if (instanceId == null
        || !instanceId.matches("[A-Za-z0-9._:-]{1,128}")
        || host == null
        || !host.matches("[A-Za-z0-9.:-]{1,253}")
        || httpPort < 1
        || httpPort > 65535
        || grpcPort < 1
        || grpcPort > 65535
        || httpPort == grpcPort
        || tlsAuthority == null
        || !tlsAuthority.matches("[a-zA-Z0-9.-]{1,253}")) {
      throw new IllegalArgumentException("Invalid separate HTTP/gRPC endpoint identity");
    }
  }

  public static String logicalName(String value) {
    String normalized = Objects.requireNonNull(value).toLowerCase(Locale.ROOT);
    if (!normalized.matches("[a-z][a-z0-9-]{0,62}"))
      throw new IllegalArgumentException("Invalid logical Cell serviceId");
    return normalized;
  }
}

package com.apex.platform.grpc.security;

import io.grpc.Metadata;
import io.grpc.ServerCall;

/** Deployment-owned authentication SPI. Claims in headers alone are not authentication. */
public interface PeerIdentityVerifier {
  Caller verify(ServerCall<?, ?> call, Metadata headers);

  record Caller(String cellId, String tenant, String principal) {
    public Caller {
      if (cellId == null
          || !cellId.matches("[A-Za-z0-9._-]{1,64}")
          || tenant == null
          || principal == null
          || tenant.length() > 128
          || principal.length() > 128
          || tenant.chars().anyMatch(Character::isISOControl)
          || principal.chars().anyMatch(Character::isISOControl))
        throw new IllegalArgumentException("Invalid verified identity");
    }
  }
}

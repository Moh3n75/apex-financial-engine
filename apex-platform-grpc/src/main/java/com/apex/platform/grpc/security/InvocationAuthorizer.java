package com.apex.platform.grpc.security;

/** Additive per-contract authorization after transport authentication, before business work. */
public interface InvocationAuthorizer {
  void authorize(
      PeerIdentityVerifier.Caller caller,
      String targetCell,
      boolean query,
      String type,
      int version);
}

package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.GatewayException;
import com.apex.platform.grpc.security.*;
import io.grpc.*;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * Exact URI SAN allowlist, fixed owning tenant and per-contract grants. No header authentication.
 */
final class CertificatePeerPolicy implements PeerIdentityVerifier, InvocationAuthorizer {
  private final PeerSecurityProperties policy;
  private final Map<String, PeerSecurityProperties.Grant> grants = new HashMap<>();

  CertificatePeerPolicy(PeerSecurityProperties policy) {
    policy.validate();
    this.policy = policy;
    policy.getPeers().values().forEach(g -> grants.put(g.uri(), g));
  }

  @Override
  public Caller verify(ServerCall<?, ?> call, Metadata headers) {
    var session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
    if (session == null) throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
    try {
      var certificate = (X509Certificate) session.getPeerCertificates()[0];
      certificate.checkValidity();
      var sans = certificate.getSubjectAlternativeNames();
      var uris =
          sans == null
              ? List.<String>of()
              : sans.stream()
                  .filter(s -> s.get(0).equals(6))
                  .map(s -> s.get(1).toString())
                  .toList();
      if (uris.size() != 1)
        throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
      var identity = PeerSecurityProperties.parse(uris.getFirst());
      if (!grants.containsKey(uris.getFirst()) || !policy.getTenant().equals(identity.tenant()))
        throw new GatewayException(GatewayException.Code.PERMISSION_DENIED, false);
      return new Caller(identity.cell(), identity.tenant(), uris.getFirst());
    } catch (GatewayException known) {
      throw known;
    } catch (Exception invalid) {
      throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
    }
  }

  @Override
  public void authorize(Caller caller, String target, boolean query, String type, int version) {
    var grant = grants.get(caller.principal());
    if (!policy.getTenant().equals(caller.tenant())
        || grant == null
        || !grant.contracts().contains((query ? "QUERY:" : "REQUEST:") + type + ":" + version))
      throw new GatewayException(GatewayException.Code.PERMISSION_DENIED, false);
  }
}

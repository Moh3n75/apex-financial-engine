package com.apex.platform.grpc.internal;

import java.net.URI;
import java.util.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Static deployment policy. Trust-chain validation remains owned by Boot's TLS transport. */
@ConfigurationProperties("apex.grpc.security")
public class PeerSecurityProperties {
  private boolean enabled;
  private String tenant;
  private Map<String, Grant> peers = Map.of();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean value) {
    enabled = value;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String value) {
    tenant = value;
  }

  public Map<String, Grant> getPeers() {
    return peers;
  }

  public void setPeers(Map<String, Grant> value) {
    peers = Map.copyOf(value);
  }

  public record Grant(String uri, List<String> contracts) {
    public Grant {
      contracts = List.copyOf(contracts);
    }
  }

  void validate() {
    if (tenant == null
        || !tenant.matches("[A-Za-z0-9._-]{1,64}")
        || peers.isEmpty()
        || peers.size() > 64)
      throw new IllegalStateException(
          "Explicit owning tenant and peer grants required for certificate security");
    var identities = new HashSet<String>();
    peers.forEach(
        (name, g) -> {
          parse(g.uri());
          if (!identities.add(g.uri())
              || g.contracts().size() > 64
              || new HashSet<>(g.contracts()).size() != g.contracts().size()
              || g.contracts().stream()
                  .anyMatch(
                      c ->
                          !c.matches(
                              "(QUERY|REQUEST):[A-Z][A-Z0-9_]{0,63}:[1-9][0-9]{0,3}|(QUERY|REQUEST):[A-Z][A-Z0-9_]{0,63}:10000")))
            throw new IllegalStateException(
                "Invalid or duplicate certificate peer/contract grant: " + name);
        });
  }

  static Identity parse(String value) {
    if (value == null
        || value.length() > 128
        || !value.matches(
            "spiffe://[a-zA-Z0-9.-]+/tenant/[A-Za-z0-9._-]{1,64}/cell/[A-Za-z0-9._-]{1,64}"))
      throw new IllegalArgumentException("Exact SPIFFE-style tenant/cell URI SAN required");
    var uri = URI.create(value);
    var parts = uri.getPath().split("/");
    return new Identity(parts[2], parts[4]);
  }

  record Identity(String tenant, String cell) {}
}

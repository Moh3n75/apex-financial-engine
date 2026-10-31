package com.apex.platform.discovery;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("apex.discovery")
public class DiscoveryProperties {
  private String provider = "none";
  private String instanceId;
  private String authority;
  private Duration refreshInterval = Duration.ofSeconds(1);
  private Map<String, Target> targets = Map.of();

  public record Target(String authority) {
    public Target {
      if (authority == null || !authority.matches("[a-zA-Z0-9.-]{1,253}"))
        throw new IllegalArgumentException("Explicit trusted TLS authority required");
    }
  }

  public String getProvider() {
    return provider;
  }

  public void setProvider(String v) {
    provider = v;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String v) {
    instanceId = v;
  }

  public String getAuthority() {
    return authority;
  }

  public void setAuthority(String v) {
    authority = v;
  }

  public Duration getRefreshInterval() {
    return refreshInterval;
  }

  public void setRefreshInterval(Duration v) {
    refreshInterval = v;
  }

  public Map<String, Target> getTargets() {
    return targets;
  }

  public void setTargets(Map<String, Target> v) {
    targets = Map.copyOf(v);
  }

  public void validate() {
    if (!java.util.Set.of("none", "eureka", "kubernetes").contains(provider)
        || refreshInterval == null
        || refreshInterval.toMillis() < 250
        || refreshInterval.compareTo(Duration.ofSeconds(30)) > 0
        || targets.size() > 32)
      throw new IllegalStateException("Invalid explicit discovery mode/refresh bounds");
    targets
        .keySet()
        .forEach(
            k -> {
              if (!CellEndpoint.logicalName(k).equals(k))
                throw new IllegalStateException("Target serviceId must be canonical lowercase");
            });
  }
}

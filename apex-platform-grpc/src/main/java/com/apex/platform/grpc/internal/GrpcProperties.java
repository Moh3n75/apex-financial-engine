package com.apex.platform.grpc.internal;

import java.time.Duration;
import java.util.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("apex.grpc")
public class GrpcProperties {
  private String cellId;
  private final Role server = new Role(), client = new Role();
  private boolean insecureLoopbackTestOnly;
  private List<String> testClients = List.of();
  private Map<String, Destination> destinations = new LinkedHashMap<>();
  private int maxPayloadBytes = 65536;
  private Duration maxDeadline = Duration.ofSeconds(30);

  public String getCellId() {
    return cellId;
  }

  public void setCellId(String v) {
    cellId = v;
  }

  public Role getServer() {
    return server;
  }

  public Role getClient() {
    return client;
  }

  public boolean isInsecureLoopbackTestOnly() {
    return insecureLoopbackTestOnly;
  }

  public void setInsecureLoopbackTestOnly(boolean v) {
    insecureLoopbackTestOnly = v;
  }

  public List<String> getTestClients() {
    return testClients;
  }

  public void setTestClients(List<String> v) {
    testClients = List.copyOf(v);
  }

  public Map<String, Destination> getDestinations() {
    return destinations;
  }

  public void setDestinations(Map<String, Destination> v) {
    destinations = Map.copyOf(v);
  }

  public int getMaxPayloadBytes() {
    return maxPayloadBytes;
  }

  public void setMaxPayloadBytes(int v) {
    maxPayloadBytes = v;
  }

  public Duration getMaxDeadline() {
    return maxDeadline;
  }

  public void setMaxDeadline(Duration v) {
    maxDeadline = v;
  }

  public void validate() {
    if (cellId == null || !cellId.matches("[a-zA-Z0-9._-]{1,64}"))
      throw new IllegalStateException("apex.grpc.cell-id (or apex.cell.id) is required");
    if (maxPayloadBytes < 128
        || maxPayloadBytes > 1048576
        || maxDeadline == null
        || maxDeadline.toMillis() < 1
        || maxDeadline.toMillis() > 30000)
      throw new IllegalStateException("Invalid gRPC size/deadline bound");
    if (destinations.size() > 32) throw new IllegalStateException("At most 32 gRPC destinations");
    if (client.isEnabled() && destinations.isEmpty())
      throw new IllegalStateException("gRPC client requires at least one explicit destination");
    destinations.forEach(
        (name, d) -> {
          if (!name.matches("[a-zA-Z0-9._-]{1,64}")
              || d == null
              || (!d.local() && (d.channel() == null || d.channel().isBlank())))
            throw new IllegalStateException("Invalid explicit gRPC destination: " + name);
        });
  }

  public static class Role {
    private boolean enabled;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean v) {
      enabled = v;
    }
  }

  public record Destination(boolean local, String channel) {}
}

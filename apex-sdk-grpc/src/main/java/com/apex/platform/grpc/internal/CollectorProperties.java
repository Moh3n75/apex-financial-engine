package com.apex.platform.grpc.internal;

import java.util.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("apex.grpc.collector")
public class CollectorProperties {
  private boolean enabled;
  private List<String> allowedResponders = List.of();
  private int maxResponders = 16, maxInFlight = 64, maxCollections = 16;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean value) {
    enabled = value;
  }

  public List<String> getAllowedResponders() {
    return allowedResponders;
  }

  public void setAllowedResponders(List<String> value) {
    allowedResponders = List.copyOf(value);
  }

  public int getMaxResponders() {
    return maxResponders;
  }

  public void setMaxResponders(int value) {
    maxResponders = value;
  }

  public int getMaxInFlight() {
    return maxInFlight;
  }

  public void setMaxInFlight(int value) {
    maxInFlight = value;
  }

  public int getMaxCollections() {
    return maxCollections;
  }

  public void setMaxCollections(int value) {
    maxCollections = value;
  }

  void validate(GrpcProperties routes) {
    if (maxResponders < 1
        || maxResponders > 16
        || maxInFlight < maxResponders
        || maxInFlight > 256
        || maxCollections < 1
        || maxCollections > 32
        || allowedResponders.isEmpty()
        || allowedResponders.size() > 32
        || new HashSet<>(allowedResponders).size() != allowedResponders.size()
        || !routes.getDestinations().keySet().containsAll(allowedResponders))
      throw new IllegalStateException(
          "Invalid bounded collector limits or explicit responder allowlist/routes");
  }
}

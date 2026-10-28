package com.apex.platform.grpc.internal;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

final class GrpcEnabled extends AnyNestedCondition {
  GrpcEnabled() {
    super(ConfigurationPhase.PARSE_CONFIGURATION);
  }

  @ConditionalOnProperty(prefix = "apex.grpc.server", name = "enabled", havingValue = "true")
  static class Server {}

  @ConditionalOnProperty(prefix = "apex.grpc.client", name = "enabled", havingValue = "true")
  static class Client {}
}

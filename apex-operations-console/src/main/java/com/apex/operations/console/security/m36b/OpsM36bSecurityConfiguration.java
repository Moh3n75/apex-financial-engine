package com.apex.operations.console.security.m36b;

import java.util.Locale;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(OpsM36bProperties.class)
@ConditionalOnProperty(prefix = "apex.ops.m36a", name = "enabled", havingValue = "true")
public class OpsM36bSecurityConfiguration {

  @Bean
  OpsM36bAccessAudit opsM36bAccessAudit() {
    return new InMemoryOpsM36bAccessAudit();
  }

  @Bean
  OpsM36bResourceAuthorizer opsM36bResourceAuthorizer(
      OpsM36bProperties props, OpsM36bAccessAudit audit) {
    if (props.isResourceAuthEnabled()) {
      validateGrants(props);
    }
    return new OpsM36bResourceAuthorizer(props, audit);
  }

  static void validateGrants(OpsM36bProperties props) {
    if (props.getResourceGrants().isEmpty()) {
      throw new IllegalStateException(
          "apex.ops.m36b.resource-auth-enabled=true requires at least one resource grant");
    }
    Set<String> kinds =
        Set.of(
            "OUTBOX_EVENT",
            "WORKFLOW",
            "KNOWN_GROUP_INBOX",
            "REQUEST_RECEIPT",
            "OVERVIEW");
    for (OpsM36bProperties.ResourceGrant g : props.getResourceGrants()) {
      if (g.getSubject() == null || g.getSubject().isBlank()) {
        throw new IllegalStateException("resource grant subject required");
      }
      if (g.getOwner() == null || g.getOwner().isBlank()) {
        throw new IllegalStateException("resource grant owner required");
      }
      if (g.getEnvironment() == null || g.getEnvironment().isBlank()) {
        throw new IllegalStateException("resource grant environment required");
      }
      for (String k : g.getKinds().split(",")) {
        String kind = k.trim().toUpperCase(Locale.ROOT);
        if (!kind.isEmpty() && !kinds.contains(kind)) {
          throw new IllegalStateException("unsupported resource kind: " + kind);
        }
      }
    }
  }
}

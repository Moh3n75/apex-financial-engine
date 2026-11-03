package com.apex.operations.console.security;

import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "apex.ops.m36a")
public class OpsM36aProperties {
  /** When true, every /ops/v1/** request requires a verified user JWT. */
  private boolean enabled = false;
  /** Exact Identity issuer origin (no path). Required when enabled. */
  private String issuer = "";
  /** Expected access-token audience. */
  private String audience = "apex-operations-console";
  /** Required OAuth2 scope (without SCOPE_ prefix). */
  private String requiredScope = "ops.console.read";
  /** Deployment tenant UUID that must match JWT tenant_id. */
  private String tenantId = "";
  /** Optional environment boundary; empty skips the claim check. */
  private String environment = "";

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getIssuer() {
    return issuer;
  }

  public void setIssuer(String issuer) {
    this.issuer = issuer;
  }

  public String getAudience() {
    return audience;
  }

  public void setAudience(String audience) {
    this.audience = audience;
  }

  public String getRequiredScope() {
    return requiredScope;
  }

  public void setRequiredScope(String requiredScope) {
    this.requiredScope = requiredScope;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getEnvironment() {
    return environment;
  }

  public void setEnvironment(String environment) {
    this.environment = environment;
  }

  public UUID tenantUuid() {
    return UUID.fromString(tenantId);
  }
}

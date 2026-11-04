package com.apex.operations.console.security.m36b;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * M36B deployment-scoped operator resource grants. Enabled only with M36A JWT validation.
 *
 * <p>Grant shape: subject (JWT sub or {@code *}) + environment + owner + kinds + scopes. Missing
 * configuration while enabled fails closed at startup.
 */
@ConfigurationProperties(prefix = "apex.ops.m36b")
public class OpsM36bProperties {
  private boolean resourceAuthEnabled = false;
  private boolean auditEnabled = true;
  private final List<ResourceGrant> resourceGrants = new ArrayList<>();

  public boolean isResourceAuthEnabled() {
    return resourceAuthEnabled;
  }

  public void setResourceAuthEnabled(boolean resourceAuthEnabled) {
    this.resourceAuthEnabled = resourceAuthEnabled;
  }

  public boolean isAuditEnabled() {
    return auditEnabled;
  }

  public void setAuditEnabled(boolean auditEnabled) {
    this.auditEnabled = auditEnabled;
  }

  public List<ResourceGrant> getResourceGrants() {
    return resourceGrants;
  }

  public static final class ResourceGrant {
    /** Exact JWT {@code sub}, or {@code *} for any authenticated user in the deployment tenant. */
    private String subject = "";
    private String environment = "test";
    private String owner = "";
    /** Comma-separated kinds: OUTBOX_EVENT,WORKFLOW,KNOWN_GROUP_INBOX,REQUEST_RECEIPT,OVERVIEW */
    private String kinds = "";
    /** Comma-separated owner scopes e.g. owner,order,reservation,authorization-simulation */
    private String scopes = "";

    public String getSubject() {
      return subject;
    }

    public void setSubject(String subject) {
      this.subject = subject;
    }

    public String getEnvironment() {
      return environment;
    }

    public void setEnvironment(String environment) {
      this.environment = environment;
    }

    public String getOwner() {
      return owner;
    }

    public void setOwner(String owner) {
      this.owner = owner;
    }

    public String getKinds() {
      return kinds;
    }

    public void setKinds(String kinds) {
      this.kinds = kinds;
    }

    public String getScopes() {
      return scopes;
    }

    public void setScopes(String scopes) {
      this.scopes = scopes;
    }
  }
}

package com.apex.operations.console.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

class OpsM36aJwtValidatorTest {
  private static final String TENANT = "11111111-1111-1111-1111-111111111111";
  private static final String AUDIENCE = "apex-operations-console";

  @Test
  void requiresIssuerAudienceTenantWhenEnabled() {
    var props = new OpsM36aProperties();
    props.setEnabled(true);
    assertThatThrownBy(() -> OpsM36aSecurityConfiguration.validateProps(props))
        .isInstanceOf(IllegalStateException.class);
    props.setIssuer("http://127.0.0.1:9000");
    props.setAudience(AUDIENCE);
    props.setRequiredScope("ops.console.read");
    props.setTenantId(TENANT);
    OpsM36aSecurityConfiguration.validateProps(props);
  }

  @Test
  void scopeConverterMapsSpaceDelimitedScopeClaim() {
    var props = new OpsM36aProperties();
    props.setRequiredScope("ops.console.read");
    var converter = OpsM36aSecurityConfiguration.opsJwtAuthenticationConverter(props);
    Jwt jwt = jwt(Map.of("scope", "openid profile ops.console.read"));
    var auth = converter.convert(jwt);
    assertThat(auth.getAuthorities())
        .extracting(Object::toString)
        .contains("SCOPE_ops.console.read");
  }

  @Test
  void audienceValidatorRejectsWrongAudience() {
    var validator = OpsM36aSecurityConfiguration.audienceValidator(AUDIENCE);
    assertThat(validator.validate(jwt(Map.of("aud", List.of(AUDIENCE)))).hasErrors()).isFalse();
    assertThat(validator.validate(jwt(Map.of("aud", List.of("apex-api")))).hasErrors()).isTrue();
  }

  @Test
  void principalTypeValidatorRequiresUser() {
    var validator = OpsM36aSecurityConfiguration.principalTypeValidator();
    assertThat(validator.validate(jwt(Map.of("principal_type", "user"))).hasErrors()).isFalse();
    assertThat(validator.validate(jwt(Map.of("principal_type", "service"))).hasErrors()).isTrue();
    assertThat(validator.validate(jwt(Map.of())).hasErrors()).isTrue();
  }

  @Test
  void tenantValidatorRequiresExactMatch() {
    var validator = OpsM36aSecurityConfiguration.tenantValidator(TENANT);
    assertThat(validator.validate(jwt(Map.of("tenant_id", TENANT))).hasErrors()).isFalse();
    assertThat(
            validator
                .validate(jwt(Map.of("tenant_id", "22222222-2222-2222-2222-222222222222")))
                .hasErrors())
        .isTrue();
  }

  @Test
  void missingScopeProducesNoOpsAuthority() {
    var props = new OpsM36aProperties();
    props.setRequiredScope("ops.console.read");
    var converter = OpsM36aSecurityConfiguration.opsJwtAuthenticationConverter(props);
    var auth = converter.convert(jwt(Map.of("scope", "openid profile api.read")));
    assertThat(auth.getAuthorities())
        .extracting(Object::toString)
        .doesNotContain("SCOPE_ops.console.read");
  }

  private static Jwt jwt(Map<String, Object> claims) {
    Instant now = Instant.now();
    Map<String, Object> headers = Map.of("alg", "none");
    Map<String, Object> body = new java.util.HashMap<>();
    body.put("sub", "user-1");
    body.putAll(claims);
    if (!body.containsKey("aud")) {
      body.put("aud", List.of(AUDIENCE));
    }
    return new Jwt("token", now, now.plusSeconds(60), headers, body);
  }
}

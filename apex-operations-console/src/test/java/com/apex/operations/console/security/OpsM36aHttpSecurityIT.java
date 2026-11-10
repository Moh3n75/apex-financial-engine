package com.apex.operations.console.security;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import com.apex.operations.console.OwnerPoller;
import com.apex.operations.console.ReadApi;
import com.apex.operations.console.TopologyApi;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = {ReadApi.class, TopologyApi.class})
@Import({OpsM36aSecurityConfiguration.class, OpsM36aHttpSecurityIT.StubDecoder.class})
@TestPropertySource(
    properties = {
      "apex.ops.m36a.enabled=true",
      "apex.ops.m36a.issuer=http://127.0.0.1:9",
      "apex.ops.m36a.audience=apex-operations-console",
      "apex.ops.m36a.required-scope=ops.console.read",
      "apex.ops.m36a.tenant-id=11111111-1111-1111-1111-111111111111",
      "spring.main.allow-bean-definition-overriding=true"
    })
class OpsM36aHttpSecurityIT {
  @Autowired MockMvc mvc;
  @MockitoBean OwnerPoller sources;

  @TestConfiguration
  static class StubDecoder {
    @Bean(name = "opsM36aJwtDecoder")
    @org.springframework.context.annotation.Primary
    JwtDecoder opsM36aJwtDecoder() {
      return token ->
          Jwt.withTokenValue(token)
              .header("alg", "none")
              .subject("stub")
              .claim("scope", "ops.console.read")
              .build();
    }
  }

  @Test
  void unauthenticatedOverviewIs401() throws Exception {
    mvc.perform(get("/ops/v1/overview")).andExpect(status().isUnauthorized());
    mvc.perform(get("/ops/v1/topology")).andExpect(status().isUnauthorized());
  }

  @Test
  void missingOpsScopeIs403() throws Exception {
    mvc.perform(
            get("/ops/v1/overview")
                .with(
                    jwt()
                        .authorities(new SimpleGrantedAuthority("SCOPE_api.read"))))
        .andExpect(status().isForbidden());
  }

  @Test
  void opsScopeAllowsGet() throws Exception {
    when(sources.views()).thenReturn(List.of());
    mvc.perform(
            get("/ops/v1/overview")
                .with(
                    jwt()
                        .authorities(
                            new SimpleGrantedAuthority("SCOPE_ops.console.read"))))
        .andExpect(status().isOk());
    mvc.perform(get("/ops/v1/topology").with(jwt().authorities(
        new SimpleGrantedAuthority("SCOPE_ops.console.read")))).andExpect(status().isOk());
    mvc.perform(get("/ops/v1/topology?schemaVersion=2").with(jwt().authorities(
        new SimpleGrantedAuthority("SCOPE_ops.console.read")))).andExpect(status().isOk())
        .andExpect(jsonPath("$.schemaVersion").value(2)).andExpect(jsonPath("$.providers").isArray());
    mvc.perform(get("/ops/v1/topology?schemaVersion=3").with(jwt().authorities(
        new SimpleGrantedAuthority("SCOPE_ops.console.read")))).andExpect(status().isBadRequest());
  }

  @Test
  void mutatingMethodDenied() throws Exception {
    // ReadOnlyFilter returns 405 before or regardless of grant; denyAll also applies.
    mvc.perform(
            post("/ops/v1/overview")
                .with(
                    jwt()
                        .authorities(
                            new SimpleGrantedAuthority("SCOPE_ops.console.read"))))
        .andExpect(status().isMethodNotAllowed());
    mvc.perform(post("/ops/v1/topology?schemaVersion=2").with(jwt().authorities(
        new SimpleGrantedAuthority("SCOPE_ops.console.read")))).andExpect(status().isMethodNotAllowed());
  }
}

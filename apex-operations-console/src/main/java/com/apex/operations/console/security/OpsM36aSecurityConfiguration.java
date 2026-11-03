package com.apex.operations.console.security;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
@EnableConfigurationProperties(OpsM36aProperties.class)
@ConditionalOnProperty(prefix = "apex.ops.m36a", name = "enabled", havingValue = "true")
public class OpsM36aSecurityConfiguration {

  @Bean
  SecurityFilterChain opsM36aSecurityFilterChain(HttpSecurity http, JwtDecoder opsM36aJwtDecoder, OpsM36aProperties props)
      throws Exception {
    validateProps(props);
    http.securityMatcher("/ops/v1/**")
        .csrf(csrf -> csrf.disable())
        .cors(cors -> cors.disable())
        .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(
            auth ->
                auth.requestMatchers(HttpMethod.GET, "/ops/v1/**")
                    .hasAuthority("SCOPE_" + props.getRequiredScope())
                    .anyRequest()
                    .denyAll())
        .oauth2ResourceServer(
            oauth ->
                oauth.jwt(
                    jwt ->
                        jwt.decoder(opsM36aJwtDecoder)
                            .jwtAuthenticationConverter(opsJwtAuthenticationConverter(props))));
    return http.build();
  }

  @Bean(name = "opsM36aJwtDecoder")
  @ConditionalOnMissingBean(name = "opsM36aJwtDecoder")
  JwtDecoder opsM36aJwtDecoder(OpsM36aProperties props) {
    validateProps(props);
    return buildJwtDecoder(props);
  }

  static void validateProps(OpsM36aProperties props) {
    if (props.getIssuer() == null || props.getIssuer().isBlank()) {
      throw new IllegalStateException("apex.ops.m36a.issuer is required when M36A is enabled");
    }
    if (props.getAudience() == null || props.getAudience().isBlank()) {
      throw new IllegalStateException("apex.ops.m36a.audience is required when M36A is enabled");
    }
    if (props.getRequiredScope() == null || props.getRequiredScope().isBlank()) {
      throw new IllegalStateException("apex.ops.m36a.required-scope is required when M36A is enabled");
    }
    if (props.getTenantId() == null || props.getTenantId().isBlank()) {
      throw new IllegalStateException("apex.ops.m36a.tenant-id is required when M36A is enabled");
    }
    props.tenantUuid();
  }

  static JwtDecoder buildJwtDecoder(OpsM36aProperties props) {
    NimbusJwtDecoder decoder = JwtDecoders.fromIssuerLocation(props.getIssuer());
    OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(props.getIssuer());
    decoder.setJwtValidator(
        new DelegatingOAuth2TokenValidator<>(
            withIssuer,
            audienceValidator(props.getAudience()),
            principalTypeValidator(),
            tenantValidator(props.getTenantId())));
    return decoder;
  }

  static OAuth2TokenValidator<Jwt> audienceValidator(String audience) {
    return token -> {
      List<String> aud = token.getAudience();
      if (aud != null && aud.contains(audience)) {
        return OAuth2TokenValidatorResult.success();
      }
      return OAuth2TokenValidatorResult.failure(
          new OAuth2Error("invalid_token", "Required audience missing", null));
    };
  }

  static OAuth2TokenValidator<Jwt> principalTypeValidator() {
    return token -> {
      Object type = token.getClaim("principal_type");
      if ("user".equals(type)) {
        return OAuth2TokenValidatorResult.success();
      }
      return OAuth2TokenValidatorResult.failure(
          new OAuth2Error("invalid_token", "principal_type must be user", null));
    };
  }

  static OAuth2TokenValidator<Jwt> tenantValidator(String tenantId) {
    return token -> {
      Object claim = token.getClaim("tenant_id");
      if (claim != null && tenantId.equals(String.valueOf(claim))) {
        return OAuth2TokenValidatorResult.success();
      }
      return OAuth2TokenValidatorResult.failure(
          new OAuth2Error("invalid_token", "tenant_id mismatch", null));
    };
  }

  static Converter<Jwt, ? extends AbstractAuthenticationToken> opsJwtAuthenticationConverter(
      OpsM36aProperties props) {
    JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(
        jwt -> {
          List<String> scopes = new java.util.ArrayList<>();
          Object scope = jwt.getClaim("scope");
          if (scope instanceof String s) {
            for (String part : s.split("\\s+")) {
              scopes.add(part);
            }
          } else if (scope instanceof Collection<?> c) {
            for (Object item : c) {
              if (item == null) {
                continue;
              }
              String value = String.valueOf(item).trim();
              // Some JWT libs put a single space-delimited string into a one-element list.
              if (value.contains(" ")) {
                for (String part : value.split("\\s+")) {
                  scopes.add(part);
                }
              } else {
                scopes.add(value);
              }
            }
          }
          return scopes.stream()
              .filter(Objects::nonNull)
              .map(s -> s.trim().toLowerCase(Locale.ROOT))
              .filter(s -> !s.isEmpty())
              .map(s -> (GrantedAuthority) new SimpleGrantedAuthority("SCOPE_" + s))
              .toList();
        });
    converter.setPrincipalClaimName(JwtClaimNames.SUB);
    return jwt -> {
      AbstractAuthenticationToken auth = converter.convert(jwt);
      if (auth instanceof JwtAuthenticationToken token) {
        return token;
      }
      return auth;
    };
  }
}

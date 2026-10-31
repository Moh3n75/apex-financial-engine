package com.apex.identity;

import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import java.util.*;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.*;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.oauth2.server.authorization.OAuth2AuthorizationServerConfigurer;
import org.springframework.security.core.userdetails.*;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.authorization.*;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.settings.AuthorizationServerSettings;
import org.springframework.security.oauth2.server.authorization.token.*;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.util.matcher.MediaTypeRequestMatcher;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
class IdentityConfiguration {
  @Bean
  org.springframework.boot.web.servlet.FilterRegistrationBean<IssuerRequestFilter> issuerOrigin(
      Environment e) {
    var bean =
        new org.springframework.boot.web.servlet.FilterRegistrationBean<>(
            new IssuerRequestFilter(e.getRequiredProperty("apex.identity.issuer")));
    bean.setOrder(-200);
    return bean;
  }

  @Bean
  UUID identityTenant(Environment e) {
    return UUID.fromString(e.getRequiredProperty("apex.identity.tenant-id"));
  }

  @Bean
  PasswordEncoder identityPasswords() {
    return PasswordEncoderFactories.createDelegatingPasswordEncoder();
  }

  @Bean
  org.springframework.security.core.session.SessionRegistry identitySessions(
      org.springframework.session.jdbc.JdbcIndexedSessionRepository sessions) {
    return new org.springframework.session.security.SpringSessionBackedSessionRegistry<>(sessions);
  }

  @Bean
  SigningKeys signingKeys(Environment e) throws Exception {
    return new SigningKeys(e);
  }

  @Bean
  JWKSource<SecurityContext> jwkSource(SigningKeys keys) {
    return keys.publicSource();
  }

  @Bean
  JwtEncoder jwtEncoder(SigningKeys keys) {
    return new NimbusJwtEncoder(keys.signingSource());
  }

  @Bean
  JwtDecoder jwtDecoder(SigningKeys keys) {
    return org.springframework.security.config.annotation.web.configuration
        .OAuth2AuthorizationServerConfiguration.jwtDecoder(keys.publicSource());
  }

  @Bean
  AuthorizationServerSettings authorizationServerSettings(Environment e) {
    return AuthorizationServerSettings.builder()
        .issuer(e.getRequiredProperty("apex.identity.issuer"))
        .build();
  }

  @Bean
  RegisteredClientRepository clients(JdbcTemplate jdbc, UUID tenant) {
    return new IdentityStores.Clients(jdbc, tenant);
  }

  @Bean
  OAuth2AuthorizationService authorizations(
      JdbcTemplate jdbc, RegisteredClientRepository clients, UUID tenant) {
    return new IdentityStores.Authorizations(jdbc, clients, tenant);
  }

  @Bean
  OAuth2AuthorizationConsentService consents(
      JdbcTemplate jdbc, RegisteredClientRepository clients) {
    return new JdbcOAuth2AuthorizationConsentService(jdbc, clients);
  }

  @Bean
  UserDetailsService users(JdbcTemplate jdbc, UUID tenant) {
    return login -> {
      var users =
          jdbc.query(
              "SELECT user_id,password_hash,enabled FROM identity_user WHERE tenant_id=? AND"
                  + " login_name=?",
              (rs, n) ->
                  User.withUsername(rs.getObject(1).toString())
                      .password(rs.getString(2))
                      .disabled(!rs.getBoolean(3))
                      .authorities("OIDC_USER")
                      .build(),
              tenant,
              login);
      if (users.size() != 1) throw new UsernameNotFoundException("Invalid login");
      return users.getFirst();
    };
  }

  @Bean
  OAuth2TokenCustomizer<JwtEncodingContext> claims(JdbcTemplate jdbc, UUID tenant) {
    return context -> {
      var policy = IdentityStores.policy(jdbc, context.getRegisteredClient().getId(), tenant);
      boolean service =
          AuthorizationGrantType.CLIENT_CREDENTIALS.equals(context.getAuthorizationGrantType());
      context
          .getClaims()
          .claim("client_id", context.getRegisteredClient().getClientId())
          .claim("tenant_id", tenant.toString())
          .claim("principal_type", service ? "service" : "user");
      if ("id_token".equals(context.getTokenType().getValue()))
        context.getClaims().expiresAt(context.getClaims().build().getIssuedAt().plusSeconds(300));
      if (OAuth2TokenType.ACCESS_TOKEN.equals(context.getTokenType()))
        context.getClaims().audience(new ArrayList<>(List.of(policy.audience())));
    };
  }

  @Bean
  SmartInitializingSingleton requireProvisionedRegistry(
      JdbcTemplate jdbc, RegisteredClientRepository clients, UUID tenant) {
    return () -> {
      var ids = jdbc.queryForList("SELECT id FROM oauth2_registered_client", String.class);
      if (ids.isEmpty()
          || jdbc.queryForObject(
                  "SELECT count(*) FROM identity_user WHERE tenant_id=? AND enabled",
                  Long.class,
                  tenant)
              == 0)
        throw new IllegalStateException("Explicit offline user/client provisioning required");
      ids.forEach(clients::findById);
    };
  }

  @Bean
  @Order(1)
  SecurityFilterChain oauth(
      HttpSecurity http,
      JdbcTemplate jdbc,
      PlatformTransactionManager tm,
      OAuth2AuthorizationService service,
      RegisteredClientRepository clients,
      UUID tenant)
      throws Exception {
    var server = new OAuth2AuthorizationServerConfigurer();
    var html = new MediaTypeRequestMatcher(MediaType.TEXT_HTML);
    html.setIgnoredMediaTypes(Set.of(MediaType.ALL));
    var grants = Set.of("authorization_code", "client_credentials", "refresh_token");
    http.securityMatcher(server.getEndpointsMatcher())
        .with(
            server,
            config ->
                config
                    .tokenEndpoint(
                        endpoint ->
                            endpoint
                                .accessTokenRequestConverters(
                                    converters ->
                                        converters.removeIf(
                                            converter ->
                                                !(converter
                                                        instanceof
                                                        org.springframework.security.oauth2.server
                                                            .authorization.web.authentication
                                                            .OAuth2AuthorizationCodeAuthenticationConverter
                                                    || converter
                                                        instanceof
                                                        org.springframework.security.oauth2.server
                                                            .authorization.web.authentication
                                                            .OAuth2ClientCredentialsAuthenticationConverter
                                                    || converter
                                                        instanceof
                                                        org.springframework.security.oauth2.server
                                                            .authorization.web.authentication
                                                            .OAuth2RefreshTokenAuthenticationConverter)))
                                .authenticationProviders(
                                    providers ->
                                        providers.removeIf(
                                            provider ->
                                                !(provider
                                                        instanceof
                                                        org.springframework.security.oauth2.server
                                                            .authorization.authentication
                                                            .OAuth2AuthorizationCodeAuthenticationProvider
                                                    || provider
                                                        instanceof
                                                        org.springframework.security.oauth2.server
                                                            .authorization.authentication
                                                            .OAuth2ClientCredentialsAuthenticationProvider
                                                    || provider
                                                        instanceof
                                                        org.springframework.security.oauth2.server
                                                            .authorization.authentication
                                                            .OAuth2RefreshTokenAuthenticationProvider))))
                    .authorizationServerMetadataEndpoint(
                        endpoint ->
                            endpoint.authorizationServerMetadataCustomizer(
                                metadata -> metadata.grantTypes(types -> types.retainAll(grants))))
                    .oidc(
                        oidc ->
                            oidc.providerConfigurationEndpoint(
                                endpoint ->
                                    endpoint.providerConfigurationCustomizer(
                                        metadata ->
                                            metadata.grantTypes(
                                                types -> types.retainAll(grants))))))
        .authorizeHttpRequests(auth -> auth.anyRequest().authenticated())
        .exceptionHandling(
            errors ->
                errors.defaultAuthenticationEntryPointFor(
                    new LoginUrlAuthenticationEntryPoint("/login"), html))
        .addFilterBefore(
            new GrantTransactionFilter(jdbc, new TransactionTemplate(tm), service),
            org.springframework.security.web.context.SecurityContextHolderFilter.class)
        .addFilterAfter(
            new AuthorizationRequestPolicyFilter(clients, jdbc, tenant),
            org.springframework.security.web.context.SecurityContextHolderFilter.class);
    return http.build();
  }

  @Bean
  @Order(2)
  SecurityFilterChain login(HttpSecurity http) throws Exception {
    http.authorizeHttpRequests(
            auth ->
                auth.dispatcherTypeMatchers(jakarta.servlet.DispatcherType.ERROR)
                    .permitAll()
                    .anyRequest()
                    .authenticated())
        .formLogin(Customizer.withDefaults());
    return http.build();
  }
}

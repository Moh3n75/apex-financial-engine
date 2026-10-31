package com.apex.identity;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.server.authorization.*;
import org.springframework.security.oauth2.server.authorization.client.*;

final class IdentityStores {
  record ClientPolicy(UUID tenant, String audience) {}

  static ClientPolicy policy(JdbcTemplate jdbc, String id, UUID tenant) {
    var rows =
        jdbc.query(
            "SELECT tenant_id,audience FROM identity_client_policy WHERE registered_client_id=?",
            (rs, n) -> new ClientPolicy(rs.getObject(1, UUID.class), rs.getString(2)),
            id);
    if (rows.size() != 1 || !tenant.equals(rows.getFirst().tenant()))
      throw new IllegalStateException("Client has no matching tenant/audience policy");
    return rows.getFirst();
  }

  static boolean enabled(JdbcTemplate jdbc, String principal, UUID tenant) {
    var rows =
        jdbc.query(
            "SELECT enabled FROM identity_user WHERE user_id::text=? AND tenant_id=? FOR SHARE",
            (rs, n) -> rs.getBoolean(1),
            principal,
            tenant);
    return rows.size() == 1 && rows.getFirst();
  }

  static final class Clients implements RegisteredClientRepository {
    private final JdbcRegisteredClientRepository delegate;
    private final JdbcTemplate jdbc;
    private final UUID tenant;

    Clients(JdbcTemplate jdbc, UUID tenant) {
      this.jdbc = jdbc;
      this.tenant = tenant;
      delegate = new JdbcRegisteredClientRepository(jdbc);
    }

    public void save(RegisteredClient client) {
      throw new UnsupportedOperationException("Offline reviewed onboarding only");
    }

    public RegisteredClient findById(String id) {
      return checked(delegate.findById(id));
    }

    public RegisteredClient findByClientId(String id) {
      return checked(delegate.findByClientId(id));
    }

    private RegisteredClient checked(RegisteredClient c) {
      if (c == null) return null;
      policy(jdbc, c.getId(), tenant);
      var grants = c.getAuthorizationGrantTypes();
      if (grants.isEmpty()
          || !java.util.Set.of(
                  AuthorizationGrantType.AUTHORIZATION_CODE,
                  AuthorizationGrantType.REFRESH_TOKEN,
                  AuthorizationGrantType.CLIENT_CREDENTIALS)
              .containsAll(grants)
          || (grants.contains(AuthorizationGrantType.CLIENT_CREDENTIALS) && grants.size() != 1)
          || (grants.contains(AuthorizationGrantType.CLIENT_CREDENTIALS)
              && c.getScopes().contains("openid"))
          || !java.util.Set.of(
                  ClientAuthenticationMethod.NONE, ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
              .containsAll(c.getClientAuthenticationMethods())
          || c.getClientAuthenticationMethods().isEmpty()
          || c.getClientAuthenticationMethods().size() != 1
          || (c.getClientAuthenticationMethods()
                  .contains(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
              && (c.getClientSecret() == null || !c.getClientSecret().startsWith("{bcrypt}")))
          || (grants.contains(AuthorizationGrantType.REFRESH_TOKEN)
              && !grants.contains(AuthorizationGrantType.AUTHORIZATION_CODE))
          || (c.getClientAuthenticationMethods().contains(ClientAuthenticationMethod.NONE)
              && (!grants.equals(java.util.Set.of(AuthorizationGrantType.AUTHORIZATION_CODE))
                  || c.getClientSecret() != null))
          || (grants.contains(AuthorizationGrantType.AUTHORIZATION_CODE)
              && (!c.getClientSettings().isRequireProofKey()
                  || !c.getClientSettings().isRequireAuthorizationConsent()))
          || c.getTokenSettings().isReuseRefreshTokens()
          || !org.springframework.security.oauth2.server.authorization.settings.OAuth2TokenFormat
              .SELF_CONTAINED
              .equals(c.getTokenSettings().getAccessTokenFormat())
          || !org.springframework.security.oauth2.jose.jws.SignatureAlgorithm.RS256.equals(
              c.getTokenSettings().getIdTokenSignatureAlgorithm())
          || c.getTokenSettings()
                  .getAccessTokenTimeToLive()
                  .compareTo(java.time.Duration.ofMinutes(5))
              > 0
          || c.getTokenSettings()
                  .getAuthorizationCodeTimeToLive()
                  .compareTo(java.time.Duration.ofMinutes(2))
              > 0
          || c.getTokenSettings()
                  .getRefreshTokenTimeToLive()
                  .compareTo(java.time.Duration.ofHours(8))
              > 0
          || c.getRedirectUris().stream().anyMatch(uri -> !safeRedirect(uri)))
        throw new IllegalStateException("Client violates approved grant/TTL/PKCE/consent policy");
      return c;
    }

    private static boolean safeRedirect(String value) {
      try {
        var uri = java.net.URI.create(value);
        return !value.contains("*")
            && uri.getHost() != null
            && uri.getUserInfo() == null
            && uri.getFragment() == null
            && ("https".equals(uri.getScheme())
                || ("http".equals(uri.getScheme())
                    && java.util.Set.of("127.0.0.1", "localhost", "[::1]")
                        .contains(uri.getHost())));
      } catch (IllegalArgumentException invalid) {
        return false;
      }
    }
  }

  static final class Authorizations implements OAuth2AuthorizationService {
    private final JdbcOAuth2AuthorizationService delegate;
    private final JdbcTemplate jdbc;
    private final UUID tenant;

    Authorizations(JdbcTemplate jdbc, RegisteredClientRepository clients, UUID tenant) {
      this.jdbc = jdbc;
      this.tenant = tenant;
      delegate = new JdbcOAuth2AuthorizationService(jdbc, clients);
    }

    public void save(OAuth2Authorization a) {
      delegate.save(a);
    }

    public void remove(OAuth2Authorization a) {
      delegate.remove(a);
    }

    public OAuth2Authorization findById(String id) {
      return allowed(delegate.findById(id));
    }

    public OAuth2Authorization findByToken(String token, OAuth2TokenType type) {
      return allowed(delegate.findByToken(token, type));
    }

    private OAuth2Authorization allowed(OAuth2Authorization a) {
      if (a == null
          || AuthorizationGrantType.CLIENT_CREDENTIALS.equals(a.getAuthorizationGrantType()))
        return a;
      return enabled(jdbc, a.getPrincipalName(), tenant) ? a : null;
    }
  }
}

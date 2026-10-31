package com.apex.identity;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.web.filter.OncePerRequestFilter;

/** Tighten the standard native-loopback port exception to this deployment's exact URI policy. */
final class AuthorizationRequestPolicyFilter extends OncePerRequestFilter {
  private final RegisteredClientRepository clients;
  private final org.springframework.jdbc.core.JdbcTemplate jdbc;
  private final java.util.UUID tenant;

  AuthorizationRequestPolicyFilter(
      RegisteredClientRepository clients,
      org.springframework.jdbc.core.JdbcTemplate jdbc,
      java.util.UUID tenant) {
    this.clients = clients;
    this.jdbc = jdbc;
    this.tenant = tenant;
  }

  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if ("/oauth2/authorize".equals(request.getServletPath())) {
      var current =
          org.springframework.security.core.context.SecurityContextHolder.getContext()
              .getAuthentication();
      if (current != null
          && current.isAuthenticated()
          && !IdentityStores.enabled(jdbc, current.getName(), tenant)) {
        response.setStatus(403);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-store");
        response.getWriter().write("{\"error\":\"access_denied\"}");
        return;
      }
    }
    if ("GET".equals(request.getMethod()) && "/oauth2/authorize".equals(request.getServletPath())) {
      boolean valid = false;
      try {
        String query = request.getQueryString();
        var client = clients.findByClientId(request.getParameter("client_id"));
        valid =
            query != null
                && query.length() <= 8192
                && client != null
                && client.getRedirectUris().contains(request.getParameter("redirect_uri"));
      } catch (RuntimeException invalid) {
        /* sanitized failure, never redirect to untrusted input */
      }
      if (!valid) {
        response.setStatus(400);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-store");
        response.getWriter().write("{\"error\":\"invalid_request\"}");
        return;
      }
    }
    chain.doFilter(request, response);
  }
}

package com.apex.identity;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationService;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

/**
 * Credential lifecycle serialization is shared PostgreSQL state; response leaves only after commit.
 */
final class GrantTransactionFilter extends OncePerRequestFilter {
  private final JdbcTemplate jdbc;
  private final TransactionTemplate transactions;
  private final OAuth2AuthorizationService authorizations;

  GrantTransactionFilter(
      JdbcTemplate jdbc, TransactionTemplate tx, OAuth2AuthorizationService authorizations) {
    this.jdbc = jdbc;
    transactions = tx;
    transactions.setIsolationLevel(
        org.springframework.transaction.TransactionDefinition.ISOLATION_READ_COMMITTED);
    transactions.setTimeout(10);
    this.authorizations = authorizations;
  }

  protected boolean shouldNotFilter(HttpServletRequest r) {
    return !"POST".equals(r.getMethod())
        || !java.util.Set.of("/oauth2/token", "/oauth2/revoke").contains(r.getServletPath());
  }

  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if (request.getContentLengthLong() > 16384) {
      response.sendError(413);
      return;
    }
    if ("authorization_code".equals(request.getParameter("grant_type"))
        && request.getParameter("code_verifier") == null) {
      response.setStatus(400);
      response.setContentType("application/json");
      response.setHeader("Cache-Control", "no-store");
      response.getWriter().write("{\"error\":\"invalid_request\"}");
      return;
    }
    var buffered = new ContentCachingResponseWrapper(response);
    try {
      transactions.executeWithoutResult(
          status -> {
            jdbc.execute("SET LOCAL lock_timeout='3s'");
            String token = request.getParameter("code");
            if (token == null) token = request.getParameter("refresh_token");
            if (token == null) token = request.getParameter("token");
            String identity = null;
            if (token != null) {
              if (token.length() > 8192) throw new IllegalArgumentException("Credential too large");
              var auth = authorizations.findByToken(token, null);
              identity = auth == null ? "credential:" + token : "authorization:" + auth.getId();
            }
            try {
              if (identity != null) {
                long lock =
                    ByteBuffer.wrap(
                            MessageDigest.getInstance("SHA-256")
                                .digest(identity.getBytes(StandardCharsets.UTF_8)))
                        .getLong();
                jdbc.execute(
                    (org.springframework.jdbc.core.PreparedStatementCreator)
                        connection -> {
                          var stmt = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)");
                          stmt.setLong(1, lock);
                          return stmt;
                        },
                    stmt -> {
                      stmt.execute();
                      return null;
                    });
              }
              chain.doFilter(request, buffered);
            } catch (Exception failure) {
              throw new IllegalStateException("Grant transaction failed", failure);
            }
          });
      buffered.copyBodyToResponse();
    } catch (RuntimeException failure) {
      buffered.resetBuffer();
      response.resetBuffer();
      response.setStatus(503);
      response.setContentType("application/json");
      response.setHeader("Cache-Control", "no-store");
      response
          .getOutputStream()
          .write("{\"error\":\"temporarily_unavailable\"}".getBytes(StandardCharsets.UTF_8));
    }
  }
}

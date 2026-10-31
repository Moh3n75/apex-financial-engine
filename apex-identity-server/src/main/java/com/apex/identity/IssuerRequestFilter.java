package com.apex.identity;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.net.URI;
import org.springframework.web.filter.OncePerRequestFilter;

/** Fixed configured external origin, never identity/forwarding assertions from caller headers. */
final class IssuerRequestFilter extends OncePerRequestFilter {
  private final URI issuer;

  IssuerRequestFilter(String issuer) {
    this.issuer = URI.create(issuer);
  }

  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    chain.doFilter(
        new HttpServletRequestWrapper(request) {
          public String getScheme() {
            return issuer.getScheme();
          }

          public String getServerName() {
            return issuer.getHost();
          }

          public int getServerPort() {
            return issuer.getPort() < 0 ? (isSecure() ? 443 : 80) : issuer.getPort();
          }

          public boolean isSecure() {
            return "https".equals(issuer.getScheme());
          }

          public StringBuffer getRequestURL() {
            return new StringBuffer(issuer.toString().replaceAll("/$", "") + getRequestURI());
          }
        },
        response);
  }
}

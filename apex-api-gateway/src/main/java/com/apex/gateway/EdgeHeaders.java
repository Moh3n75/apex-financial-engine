package com.apex.gateway;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.util.*;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

/** Trace IDs are not identity. No caller-supplied security or forwarding assertion is trusted. */
final class EdgeHeaders extends OncePerRequestFilter implements Ordered {
  public int getOrder() {
    return Ordered.HIGHEST_PRECEDENCE;
  }

  static boolean strip(String name) {
    String n = name.toLowerCase(Locale.ROOT);
    return n.startsWith("x-forwarded-")
        || n.startsWith("x-cell-")
        || n.startsWith("x-apex-")
        || n.startsWith("x-user")
        || n.startsWith("x-principal")
        || n.startsWith("x-tenant")
        || Set.of(
                "forwarded",
                "authorization",
                "proxy-authorization",
                "cookie",
                "x-request-id",
                "x-correlation-id")
            .contains(n);
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest incoming, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    String candidate = incoming.getHeader("X-Correlation-ID");
    String correlation;
    try {
      correlation = UUID.fromString(candidate).toString();
    } catch (RuntimeException malformed) {
      correlation = UUID.randomUUID().toString();
    }
    Map<String, String> trace =
        Map.of("x-correlation-id", correlation, "x-request-id", UUID.randomUUID().toString());
    response.setHeader("X-Correlation-ID", correlation);
    response.setHeader("X-Request-ID", trace.get("x-request-id"));
    var safe =
        new HttpServletRequestWrapper(incoming) {
          public String getHeader(String name) {
            String lower = name.toLowerCase(Locale.ROOT);
            return trace.containsKey(lower)
                ? trace.get(lower)
                : strip(name) ? null : super.getHeader(name);
          }

          public Enumeration<String> getHeaders(String name) {
            String value = trace.get(name.toLowerCase(Locale.ROOT));
            return value != null
                ? Collections.enumeration(List.of(value))
                : strip(name) ? Collections.emptyEnumeration() : super.getHeaders(name);
          }

          public Enumeration<String> getHeaderNames() {
            var names = new LinkedHashSet<String>();
            Collections.list(super.getHeaderNames()).stream()
                .filter(n -> !strip(n))
                .forEach(names::add);
            names.addAll(trace.keySet());
            return Collections.enumeration(names);
          }
        };
    chain.doFilter(safe, response);
  }
}

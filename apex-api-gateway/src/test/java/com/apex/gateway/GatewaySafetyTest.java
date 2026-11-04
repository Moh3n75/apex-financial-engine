package com.apex.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

@Tag("unit")
class GatewaySafetyTest {

  @Test
  void gatewayRemainsASimpleSpringBootEntryPoint() {
    assertThat(GatewayApplication.class.isAnnotationPresent(SpringBootApplication.class))
            .isTrue();
  }

  @Test
  void edgeHeadersStripsUntrustedHeadersAndReplacesCallerRequestId() throws Exception {
    var incoming = new MockHttpServletRequest();
    var response = new MockHttpServletResponse();
    String correlation = UUID.randomUUID().toString();
    incoming.addHeader("X-Correlation-ID", correlation);
    incoming.addHeader("X-Request-ID", "forged");

    var untrustedHeaders = List.of(
            "Authorization", "Cookie", "X-User", "X-Tenant", "X-Cell-Id",
            "Forwarded", "X-Forwarded-Host", "X-Apex-Instance");
    for (String header : untrustedHeaders) {
      incoming.addHeader(header, "forged");
    }

    new EdgeHeaders().doFilter(incoming, response, (request, reply) -> {
      var safe = (HttpServletRequest) request;
      for (String header : untrustedHeaders) {
        assertThat(safe.getHeader(header)).isNull();
        assertThat(Collections.list(safe.getHeaders(header))).isEmpty();
        assertThat(Collections.list(safe.getHeaderNames())).doesNotContain(header);
      }

      assertThat(safe.getHeader("X-Correlation-ID")).isEqualTo(correlation);
      String requestId = safe.getHeader("X-Request-ID");
      assertThat(UUID.fromString(requestId)).isNotNull();
      assertThat(requestId).isNotEqualTo("forged");
    });

    assertThat(response.getHeader("X-Correlation-ID")).isEqualTo(correlation);
    assertThat(UUID.fromString(response.getHeader("X-Request-ID"))).isNotNull();
  }

  @Test
  void edgeHeadersReplacesMalformedCorrelationId() throws Exception {
    var request = new MockHttpServletRequest();
    request.addHeader("X-Correlation-ID", "bad");
    var response = new MockHttpServletResponse();

    new EdgeHeaders().doFilter(request, response, (wrapped, ignored) -> {
      String correlation = ((HttpServletRequest) wrapped).getHeader("X-Correlation-ID");
      assertThat(UUID.fromString(correlation)).isNotNull();
      assertThat(correlation).isNotEqualTo("bad");
    });

    assertThat(UUID.fromString(response.getHeader("X-Correlation-ID"))).isNotNull();
    assertThat(response.getHeader("X-Correlation-ID")).isNotEqualTo("bad");
  }
}

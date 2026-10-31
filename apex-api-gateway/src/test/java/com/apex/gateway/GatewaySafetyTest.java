package com.apex.gateway;

import static org.assertj.core.api.Assertions.*;

import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.core.env.*;
import org.springframework.mock.web.*;

@Tag("unit")
class GatewaySafetyTest {
  @Test
  void missingBackendKeeps503AndDoesNotExposeInternalDiagnostics() throws Exception {
    var request =
        org.springframework.web.servlet.function.ServerRequest.create(
            new MockHttpServletRequest(), List.of());
    var response =
        GatewayApplication.upstreamFailures()
            .filter(
                request,
                ignored -> {
                  throw new org.springframework.web.client.HttpServerErrorException(
                      org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE,
                      "internal diagnostic must not escape");
                });
    assertThat(response.statusCode().value()).isEqualTo(503);
    assertThat(((org.springframework.web.servlet.function.EntityResponse<?>) response).entity())
        .isEqualTo(Map.of("code", "UPSTREAM_UNAVAILABLE", "outcome", "UNKNOWN"));
  }

  @Test
  void timedOutWriteRemainsUnknownAndIsNeverAutomaticallyRetried() throws Exception {
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var request =
        org.springframework.web.servlet.function.ServerRequest.create(
            new MockHttpServletRequest(), List.of());
    var response =
        GatewayApplication.upstreamFailures()
            .filter(
                request,
                ignored -> {
                  calls.incrementAndGet();
                  throw new org.springframework.web.client.ResourceAccessException(
                      "owned transport timeout");
                });
    assertThat(response.statusCode().value()).isEqualTo(504);
    assertThat(calls.get()).isEqualTo(1);
    assertThat(((org.springframework.web.servlet.function.EntityResponse<?>) response).entity())
        .isEqualTo(Map.of("code", "UPSTREAM_TRANSPORT_FAILURE", "outcome", "UNKNOWN"));
  }

  @Test
  void publicAndDefaultExposureFailClosed() {
    for (var props :
        List.<Map<String, Object>>of(
            Map.of(),
            Map.of("apex.gateway.mode", "loopback-development", "server.address", "0.0.0.0"))) {
      var e = new StandardEnvironment();
      e.getPropertySources().addFirst(new MapPropertySource("test", props));
      assertThatThrownBy(() -> GatewayApplication.validateExposure(e))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  void onlyExplicitLoopbackDevelopmentIsEnabled() {
    var e = new StandardEnvironment();
    e.getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test",
                Map.of(
                    "apex.gateway.mode", "loopback-development", "server.address", "127.0.0.1")));
    assertThatCode(() -> GatewayApplication.validateExposure(e)).doesNotThrowAnyException();
  }

  @Test
  void stripsSecurityForwardingAndCallerRequestIdentityButPreservesValidCorrelation()
      throws Exception {
    var incoming = new MockHttpServletRequest();
    var response = new MockHttpServletResponse();
    String correlation = UUID.randomUUID().toString();
    incoming.addHeader("X-Correlation-ID", correlation);
    incoming.addHeader("X-Request-ID", "forged");
    for (String name :
        List.of(
            "Authorization",
            "Cookie",
            "X-User",
            "X-Tenant",
            "X-Cell-Id",
            "Forwarded",
            "X-Forwarded-Host",
            "X-Apex-Instance")) incoming.addHeader(name, "forged");
    new EdgeHeaders()
        .doFilter(
            incoming,
            response,
            (request, reply) -> {
              var safe = (HttpServletRequest) request;
              for (String name :
                  List.of(
                      "Authorization",
                      "Cookie",
                      "X-User",
                      "X-Tenant",
                      "X-Cell-Id",
                      "Forwarded",
                      "X-Forwarded-Host",
                      "X-Apex-Instance")) {
                assertThat(safe.getHeader(name)).isNull();
                assertThat(Collections.list(safe.getHeaders(name))).isEmpty();
                assertThat(Collections.list(safe.getHeaderNames())).doesNotContain(name);
              }
              assertThat(safe.getHeader("X-Correlation-ID")).isEqualTo(correlation);
              assertThat(UUID.fromString(safe.getHeader("X-Request-ID"))).isNotNull();
              assertThat(safe.getHeader("X-Request-ID")).isNotEqualTo("forged");
            });
    assertThat(response.getHeader("X-Correlation-ID")).isEqualTo(correlation);
  }

  @Test
  void malformedTraceIsReplacedWithoutBecomingAuthentication() throws Exception {
    var request = new MockHttpServletRequest();
    request.addHeader("X-Correlation-ID", "bad");
    var response = new MockHttpServletResponse();
    new EdgeHeaders()
        .doFilter(
            request,
            response,
            (r, s) ->
                assertThat(UUID.fromString(((HttpServletRequest) r).getHeader("X-Correlation-ID")))
                    .isNotNull());
    assertThat(response.getHeader("X-Correlation-ID")).isNotEqualTo("bad");
  }
}

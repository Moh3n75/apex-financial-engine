package com.apex.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;

@Tag("unit")
class GatewaySafetyTest {

  @Test
  void gatewayRemainsASimpleSpringBootEntryPoint() {
    assertThat(GatewayApplication.class.isAnnotationPresent(SpringBootApplication.class))
            .isTrue();
  }

  @Test
  void edgeHeadersStripsUntrustedHeadersAndReplacesCallerRequestId() {
    String correlation = UUID.randomUUID().toString();
    List<String> untrustedHeaders = List.of(
            "Authorization", "Cookie", "Proxy-Authorization",
            "X-User", "X-Principal-Id", "X-Tenant", "X-Cell-Id",
            "Forwarded", "X-Forwarded-Host", "X-Original-URL",
            "X-Real-IP", "X-Apex-Instance");

    var request = MockServerHttpRequest.post("/api/v1/order-placements")
            .header("X-Correlation-ID", correlation)
            .header("X-Request-ID", "forged")
            .header("X-Allowed-Header", "preserve-me");
    for (String name : untrustedHeaders) {
      request.header(name, "forged");
    }

    var exchange = MockServerWebExchange.from(request);
    var receivedRequestId = new AtomicReference<String>();

    WebFilterChain downstream = safeExchange -> {
      HttpHeaders headers = safeExchange.getRequest().getHeaders();

      for (String name : untrustedHeaders) {
        assertThat(headers.getFirst(name)).isNull();
        assertThat(headers.headerNames())
                .noneMatch(candidate -> candidate.equalsIgnoreCase(name));
      }

      assertThat(headers.getFirst("X-Allowed-Header"))
              .isEqualTo("preserve-me");
      assertThat(headers.getFirst("X-Correlation-ID"))
              .isEqualTo(correlation);

      String requestId = headers.getFirst("X-Request-ID");
      assertThat(requestId).isNotNull().isNotEqualTo("forged");
      assertThat(UUID.fromString(requestId).toString()).isEqualTo(requestId);
      receivedRequestId.set(requestId);

      // Ensure the ingress-selected trace headers survive response commit.
      safeExchange.getResponse().getHeaders().set("X-Correlation-ID", "upstream-value");
      safeExchange.getResponse().getHeaders().set("X-Request-ID", "upstream-value");
      return safeExchange.getResponse().setComplete();
    };

    new EdgeHeaders().filter(exchange, downstream).block();

    assertThat(exchange.getResponse().isCommitted()).isTrue();
    assertThat(exchange.getResponse().getHeaders().getFirst("X-Correlation-ID"))
            .isEqualTo(correlation);
    assertThat(exchange.getResponse().getHeaders().getFirst("X-Request-ID"))
            .isEqualTo(receivedRequestId.get());
  }

  @Test
  void edgeHeadersReplacesMalformedCorrelationId() {
    var exchange = MockServerWebExchange.from(
            MockServerHttpRequest.get("/api/v1/order-placements/123")
                    .header("X-Correlation-ID", "bad")
                    .header("X-Request-ID", "caller-controlled"));
    var receivedCorrelation = new AtomicReference<String>();
    var receivedRequestId = new AtomicReference<String>();

    WebFilterChain downstream = safeExchange -> {
      HttpHeaders headers = safeExchange.getRequest().getHeaders();
      String correlation = headers.getFirst("X-Correlation-ID");
      String requestId = headers.getFirst("X-Request-ID");

      assertThat(correlation).isNotEqualTo("bad");
      assertThat(UUID.fromString(correlation).toString()).isEqualTo(correlation);
      assertThat(requestId).isNotEqualTo("caller-controlled");
      assertThat(UUID.fromString(requestId).toString()).isEqualTo(requestId);
      receivedCorrelation.set(correlation);
      receivedRequestId.set(requestId);

      return safeExchange.getResponse().setComplete();
    };

    new EdgeHeaders().filter(exchange, downstream).block();

    assertThat(exchange.getResponse().getHeaders().getFirst("X-Correlation-ID"))
            .isEqualTo(receivedCorrelation.get());
    assertThat(exchange.getResponse().getHeaders().getFirst("X-Request-ID"))
            .isEqualTo(receivedRequestId.get());
  }

  @Test
  void edgeHeadersRejectsNonCanonicalCorrelationAndCaseVariantSpoofing() {
    String canonical = UUID.randomUUID().toString();
    var exchange = MockServerWebExchange.from(
            MockServerHttpRequest.get("/api/v1/order-placements/123")
                    .header("x-correlation-id", canonical.replace("-", ""))
                    .header("x-CeLl-Identity", "forged")
                    .header("X-aPeX-User", "forged")
                    .header("x-fOrWaRdEd-fOr", "forged"));

    WebFilterChain downstream = safeExchange -> {
      HttpHeaders headers = safeExchange.getRequest().getHeaders();
      assertThat(headers.getFirst("X-Cell-Identity")).isNull();
      assertThat(headers.getFirst("X-Apex-User")).isNull();
      assertThat(headers.getFirst("X-Forwarded-For")).isNull();
      assertThat(headers.getFirst("X-Correlation-ID"))
              .isNotEqualTo(canonical.replace("-", ""));
      assertThat(UUID.fromString(headers.getFirst("X-Correlation-ID"))).isNotNull();
      return safeExchange.getResponse().setComplete();
    };

    new EdgeHeaders().filter(exchange, downstream).block();
  }
}

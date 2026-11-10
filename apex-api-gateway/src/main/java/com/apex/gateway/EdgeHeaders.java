
package com.apex.gateway;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import reactor.core.publisher.Mono;

/**
 * Gateway ingress header policy for Spring Cloud Gateway Server WebFlux.
 *
 * Trace IDs are not authentication or authorization. Untrusted caller-supplied
 * identity, credentials, and forwarding headers are never passed downstream.
 *
 * This filter must be registered as a Spring WebFilter bean.
 */
final class EdgeHeaders implements WebFilter, Ordered {

    private static final String CORRELATION_ID = "X-Correlation-ID";
    private static final String REQUEST_ID = "X-Request-ID";

    private static final Set<String> STRIPPED_HEADERS = Set.of(
            "forwarded",
            "authorization",
            "proxy-authorization",
            "cookie",
            "x-request-id",
            "x-correlation-id",
            "x-real-ip"
    );

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    static boolean strip(String name) {
        String lower = name.toLowerCase(Locale.ROOT);

        return lower.startsWith("x-forwarded-")
                || lower.startsWith("x-original-")
                || lower.startsWith("x-cell-")
                || lower.startsWith("x-apex-")
                || lower.startsWith("x-user")
                || lower.startsWith("x-principal")
                || lower.startsWith("x-tenant")
                || STRIPPED_HEADERS.contains(lower);
    }

    @Override
    public Mono<Void> filter(
            ServerWebExchange exchange,
            WebFilterChain chain) {

        String correlationId = validCorrelationId(
                exchange.getRequest()
                        .getHeaders()
                        .getFirst(CORRELATION_ID));

        String requestId = UUID.randomUUID().toString();


        ServerHttpRequest sanitized = exchange.getRequest()
                .mutate()
                .headers(headers -> {

                    List.copyOf(headers.headerNames())
                            .stream()
                            .filter(EdgeHeaders::strip)
                            .forEach(headers::remove);

                    headers.set(CORRELATION_ID, correlationId);
                    headers.set(REQUEST_ID, requestId);
                })
                .build();


        exchange.getResponse().beforeCommit(() -> {
            HttpHeaders headers = exchange.getResponse().getHeaders();

            headers.set(CORRELATION_ID, correlationId);
            headers.set(REQUEST_ID, requestId);

            return Mono.empty();
        });

        return chain.filter(
                exchange.mutate()
                        .request(sanitized)
                        .build()
        );
    }

    private static String validCorrelationId(String candidate) {
        if (candidate != null && candidate.length() == 36) {
            try {
                String canonical = UUID.fromString(candidate).toString();

                if (canonical.equalsIgnoreCase(candidate)) {
                    return canonical;
                }
            } catch (IllegalArgumentException ignored) {
                // Replace invalid caller-provided trace IDs.
            }
        }

        return UUID.randomUUID().toString();
    }
}

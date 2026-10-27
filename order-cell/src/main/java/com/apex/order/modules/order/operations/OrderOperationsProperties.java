package com.apex.order.modules.order.operations;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Explicit read-only operator access; the supplied token is never an authenticated actor identity. */
@ConfigurationProperties("apex.order.operations")
public final class OrderOperationsProperties {
    private final Duration ageThreshold;
    private final byte[] tokenDigest;

    public OrderOperationsProperties(@DefaultValue("15m") Duration ageThreshold, String readToken) {
        if (ageThreshold == null || ageThreshold.toMillis() < 1 || ageThreshold.compareTo(Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException("Operator age threshold must be between 1ms and 365d");
        }
        if (readToken == null || readToken.length() < 24 || readToken.length() > 500 || readToken.contains("${")
                || readToken.chars().anyMatch(c -> Character.isISOControl(c) || Character.isWhitespace(c))) {
            throw new IllegalArgumentException("Explicit operator read token of at least 24 characters is required");
        }
        this.ageThreshold = ageThreshold;
        this.tokenDigest = digest(readToken);
    }

    public Duration ageThreshold() {
        return ageThreshold;
    }

    public boolean authorizes(String authorization) {
        return authorization != null && authorization.length() <= 1024 && authorization.startsWith("Bearer ")
                && MessageDigest.isEqual(tokenDigest, digest(authorization.substring(7)));
    }

    private static byte[] digest(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}

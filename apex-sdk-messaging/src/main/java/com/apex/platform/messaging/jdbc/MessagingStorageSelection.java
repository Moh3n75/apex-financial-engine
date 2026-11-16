package com.apex.platform.messaging.jdbc;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

/** Internal selection policy; selecting a provider is distinct from identifying a live database. */
class MessagingStorageSelection extends SpringBootCondition {
    static String provider(Environment environment, ClassLoader loader) {
        String fallback = loader.getResource("META-INF/apex/messaging-core.properties") == null
                ? "postgresql" : "custom";
        String value = environment.getProperty("apex.messaging.storage.provider", fallback);
        if (!value.equals("postgresql") && !value.equals("custom"))
            throw new IllegalArgumentException("Unsupported apex.messaging.storage.provider: " + value
                    + "; supported values are postgresql and custom (owner-provided storage)");
        return value;
    }

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        // Validate invalid values in the existing selected-messaging preflight, not inactive imports.
        String value = context.getEnvironment().getProperty("apex.messaging.storage.provider");
        boolean selected = value == null
                ? context.getClassLoader().getResource("META-INF/apex/messaging-core.properties") == null
                : value.equals("postgresql");
        return new ConditionOutcome(selected, "PostgreSQL storage explicitly selected or legacy default");
    }

    static void requirePostgresql(JdbcTemplate jdbc) {
        // JdbcTemplate participates in the owner's existing transaction. No connection on construction,
        // no datasource/manager replacement and no cached product assumption across routed datasources.
        jdbc.execute((ConnectionCallback<Void>) connection -> {
            String product = connection.getMetaData().getDatabaseProductName();
            if (!"PostgreSQL".equals(product))
                throw new IllegalStateException("PostgreSQL messaging storage requires database product PostgreSQL; found " + product);
            return null;
        });
    }
}

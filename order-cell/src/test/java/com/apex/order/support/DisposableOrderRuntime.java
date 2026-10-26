package com.apex.order.support;

import java.io.IOException;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import com.apex.order.OrderCellApplication;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/** Test-only real resources; the production application defines no infrastructure Beans. */
public final class DisposableOrderRuntime implements AutoCloseable {
    public static final String TOPIC = "apex.order.events.v1";

    private EmbeddedPostgres database;
    private EmbeddedKafkaKraftBroker broker;
    private ConfigurableApplicationContext application;

    private DisposableOrderRuntime() {}

    public static DisposableOrderRuntime start() throws Exception {
        return start(false);
    }

    public static DisposableOrderRuntime start(boolean sagaConsumer) throws Exception {
        DisposableOrderRuntime runtime = new DisposableOrderRuntime();
        try {
            runtime.database = EmbeddedPostgres.builder().start();
            runtime.broker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC);
            runtime.broker.brokerProperty("auto.create.topics.enable", "false");
            runtime.broker.brokerListProperty("apex.m24.test.brokers");
            runtime.broker.afterPropertiesSet();

            StandardEnvironment environment = new StandardEnvironment();
            environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
            environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
            runtime.application = new SpringApplicationBuilder(OrderCellApplication.class)
                    .environment(environment)
                    .run("--server.port=0", "--spring.config.location=classpath:application.yml",
                            "--spring.config.additional-location=", "--spring.config.import=",
                            "--spring.profiles.active=", "--spring.profiles.include=",
                            "--spring.datasource.url=" + runtime.database.getJdbcUrl("postgres", "postgres"),
                            "--spring.datasource.username=postgres", "--spring.datasource.password=",
                            "--spring.kafka.bootstrap-servers=" + runtime.broker.getBrokersAsString(),
                            "--apex.cell.id=order-cell", "--apex.messaging.publisher.topic=" + TOPIC,
                            "--apex.messaging.cell.enabled=true", "--apex.messaging.consumer.enabled=" + sagaConsumer,
                            "--spring.kafka.consumer.group-id=order-cell-test-" + java.util.UUID.randomUUID(),
                            "--apex.messaging.consumer.topics=apex.inventory.events.v1",
                            "--spring.kafka.listener.auto-startup=false",
                            "--spring.kafka.consumer.enable-auto-commit=false",
                            "--spring.kafka.listener.ack-mode=manual_immediate",
                            "--spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                            "--spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                            "--apex.messaging.outbox.publisher.enabled=true",
                            "--apex.messaging.outbox.publisher.interval-ms=100");
            return runtime;
        } catch (Exception | Error failure) {
            try {
                runtime.close();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    public ConfigurableApplicationContext application() {
        return application;
    }

    public int port() {
        return ((WebServerApplicationContext) application).getWebServer().getPort();
    }

    public String brokers() {
        return broker.getBrokersAsString();
    }

    @Override
    public void close() throws IOException {
        try {
            if (application != null) {
                application.close();
            }
        } finally {
            try {
                if (broker != null) {
                    broker.destroy();
                }
            } finally {
                if (database != null) {
                    database.close();
                }
            }
        }
    }
}

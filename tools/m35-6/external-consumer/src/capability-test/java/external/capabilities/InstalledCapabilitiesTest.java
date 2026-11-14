package external.capabilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.events.api.EventContracts;
import com.apex.platform.grpc.api.CellQuery;
import com.apex.platform.grpc.api.CellQueryHandler;
import com.apex.platform.grpc.api.QueryGateway;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class InstalledCapabilitiesTest {
    private static final AtomicInteger PORT = new AtomicInteger();
    @CellQuery(type = "EXTERNAL_READ", version = 1, response = Reply.class)
    public record Read(String value) {}
    public record Reply(String value) {}
    @CellQuery(type = "EXTERNAL_SLOW_READ", version = 1, response = Reply.class)
    public record SlowRead(int delayMillis) {}
    public record Changed(String value) implements com.apex.platform.events.IntegrationEvent {}

    public static class Queries {
        @CellQueryHandler(type = "EXTERNAL_SLOW_READ", version = 1)
        public Reply slow(SlowRead query) throws InterruptedException {
            Thread.sleep(query.delayMillis());
            return new Reply("slow");
        }
        @CellQueryHandler(type = "EXTERNAL_READ", version = 1)
        public Reply read(Read query) { return new Reply(query.value()); }
    }

    @Test
    void installedCollectorExpiresRepeatedNativeCalls() throws Exception {
        String noDb = "--spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration";
        try (var server = boot(noDb, "--apex.grpc.server.enabled=true", "--spring.grpc.server.enabled=true",
                "--apex.grpc.cell-id=external-server", "--apex.grpc.insecure-loopback-test-only=true",
                "--apex.grpc.test-clients[0]=external-client", "--spring.grpc.server.address=127.0.0.1",
                "--spring.grpc.server.port=0");
             var client = boot(noDb, "--apex.grpc.client.enabled=true", "--apex.grpc.cell-id=external-client",
                     "--apex.grpc.insecure-loopback-test-only=true", "--apex.grpc.destinations.external-server.channel=server",
                     "--spring.grpc.client.channel.server.target=static://127.0.0.1:" + PORT.get(),
                     "--spring.grpc.client.channel.server.ssl.enabled=false", "--apex.grpc.collector.enabled=true",
                     "--apex.grpc.collector.allowed-responders=external-server")) {
            var gateway = client.getBean(com.apex.platform.grpc.api.MultiResponderGateway.class);
            for (int i=0;i<100;i++) {
                var result = gateway.collectQueries(new SlowRead(80), Reply.class,
                        new com.apex.platform.grpc.api.CollectionOptions(List.of("external-server"), Duration.ofMillis(25), true))
                        .toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(result.responders()).hasSize(1);
                assertThat(result.responders().getFirst().status())
                        .isEqualTo(com.apex.platform.grpc.api.ResponderResult.Status.TIMED_OUT);
            }
        }
    }

    @Test
    void installedOperationsReadsAndEnforcesSqlDeadline() throws Exception {
        try (var pg=EmbeddedPostgres.builder().start(); var pool=new com.zaxxer.hikari.HikariDataSource()) {
            pool.setDataSource(pg.getPostgresDatabase()); pool.setMaximumPoolSize(4);
            Flyway.configure().dataSource(pool).locations("classpath:db/platform").load().migrate();
            var jdbc=new JdbcTemplate(pool);
            jdbc.execute("CREATE INDEX ops03d_outbox_event_recent ON outbox_event(created_at DESC,id DESC)");
            jdbc.execute("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload,created_at) VALUES (gen_random_uuid(),'EXTERNAL',1,gen_random_uuid(),gen_random_uuid(),'EXTERNAL','external','external',now(),'{}',now())");
            var now=java.time.Instant.now(); var clock=java.time.Clock.fixed(now,java.time.ZoneOffset.UTC);
            var searchKind=com.apex.operations.contracts.OwnerSearch.Kind.OUTBOX_EVENT;
            var searchQuery=new com.apex.operations.contracts.OwnerSearch.Query("test","external",searchKind,"external",
                    now.minusSeconds(300),now.plusSeconds(1),"ANY",16,"");
            var rate=new java.util.concurrent.atomic.AtomicLong();
            var search=new com.apex.operations.owner.BoundedOwnerSearch(pool,"test","external","one",
                    Map.of(searchKind,java.util.Set.of("external")),Map.of(searchKind,com.apex.operations.owner.MessagingSearchSource.OUTBOX_STATES),
                    com.apex.operations.owner.MessagingSearchSource::read,clock,()->rate.addAndGet(2_000_000_000L));
            assertThat(search.read(searchQuery).items()).hasSize(1);
            var recentKind=com.apex.operations.contracts.OwnerRecentSearch.Kind.OUTBOX_EVENT;
            var recentQuery=new com.apex.operations.contracts.OwnerRecentSearch.Query("test","external",recentKind,"external",
                    now.minusSeconds(300),now.plusSeconds(1),"ANY",16,"");
            var recent=new com.apex.operations.owner.BoundedRecentSearch(pool,"test","external","one",
                    Map.of(recentKind,java.util.Set.of("external")),Map.of(recentKind,com.apex.operations.owner.MessagingRecentSource.OUTBOX_STATES),
                    com.apex.operations.owner.MessagingRecentSource::read,clock,()->rate.addAndGet(2_000_000_000L));
            assertThat(recent.read(recentQuery).items()).hasSize(1);
            var slow=new com.apex.operations.owner.BoundedRecentSearch(pool,"test","external","one",
                    Map.of(recentKind,java.util.Set.of("external")),Map.of(recentKind,com.apex.operations.owner.MessagingRecentSource.OUTBOX_STATES),
                    (connection,query,position)->{try(var statement=connection.createStatement()){statement.execute("SELECT pg_sleep(2)");}return List.of();},
                    clock,()->rate.addAndGet(2_000_000_000L));
            assertThatThrownBy(()->slow.read(recentQuery)).isInstanceOfSatisfying(
                    com.apex.platform.grpc.api.GatewayException.class,
                    error->assertThat(error.code()).isEqualTo(com.apex.platform.grpc.api.GatewayException.Code.DEADLINE_EXCEEDED));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @Import(Queries.class)
    static class App {
        @Bean ApplicationListener<GrpcServerStartedEvent> port() {
            return event -> PORT.set(event.getPort());
        }
    }

    private ConfigurableApplicationContext boot(String... extra) {
        var args = new ArrayList<>(List.of("--spring.config.location=optional:classpath:/absent.properties",
                "--spring.main.banner-mode=off", "--spring.application.name=external-consumer",
                "--apex.cell.id=external-consumer"));
        if (List.of(extra).stream().noneMatch(value -> value.startsWith("--spring.grpc.server.enabled="))) {
            args.add("--spring.grpc.server.enabled=false");
        }
        args.addAll(List.of(extra));
        var app = new SpringApplication(App.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        return app.run(args.toArray(String[]::new));
    }

    @Test
    void installedMessagingPublishesTransactionalOutboxToOwnedKafka() throws Exception {
        String topic = "external-" + UUID.randomUUID();
        var broker = new EmbeddedKafkaKraftBroker(1, 1);
        broker.brokerProperties(Map.of("auto.create.topics.enable", "false"));
        try (var pg = EmbeddedPostgres.builder().start()) {
            broker.afterPropertiesSet();
            String url = pg.getJdbcUrl("postgres", "postgres");
            Flyway.configure().dataSource(url, "postgres", "").locations("classpath:db/platform").load().migrate();
            try (var context = boot("--spring.datasource.url=" + url, "--spring.datasource.username=postgres",
                    "--spring.datasource.password=", "--apex.messaging.cell.enabled=true",
                    "--apex.messaging.provisioning.enabled=true", "--spring.kafka.admin.auto-create=false",
                    "--apex.messaging.consumer.enabled=false", "--apex.messaging.publisher.topic=" + topic,
                    "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                    "--spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                    "--spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
                    "--spring.kafka.producer.acks=all")) {
                var jdbc = context.getBean(JdbcTemplate.class);
                UUID aggregate = UUID.randomUUID();
                var execution = ExecutionContext.root("external-consumer", "external-consumer");
                try (var scope = context.getBean(ExecutionContextProvider.class).open(
                        execution)) {
                    new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).executeWithoutResult(status ->
                            context.getBean(CellEvents.class).append(EventContracts.define("EXTERNAL_CHANGED", 1,
                                    "EXAMPLE", Changed.class, "example.changed.v1"), aggregate, new Changed("safe")));
                }
                await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE published", Integer.class)).isEqualTo(1));
                try (var consumer = new KafkaConsumer<String, String>(Map.of("bootstrap.servers", broker.getBrokersAsString(),
                        "key.deserializer", StringDeserializer.class, "value.deserializer", StringDeserializer.class,
                        "enable.auto.commit", false))) {
                    var partition = new TopicPartition(topic, 0);
                    consumer.assign(List.of(partition));
                    consumer.seek(partition, 0);
                    var observed = new java.util.concurrent.atomic.AtomicReference<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
                    await().atMost(Duration.ofSeconds(10)).until(() -> {
                        var records = consumer.poll(Duration.ofMillis(100));
                        if (records.isEmpty()) return false;
                        assertThat(records.count()).isEqualTo(1);
                        observed.set(records.iterator().next());
                        return true;
                    });
                    assertThat(observed.get().key()).isEqualTo("EXAMPLE:" + aggregate);
                    var envelope = context.getBean(tools.jackson.databind.json.JsonMapper.class).readTree(observed.get().value());
                    assertThat(envelope.path("metadata").path("eventType").asText()).isEqualTo("EXTERNAL_CHANGED");
                    assertThat(envelope.path("metadata").path("eventVersion").asInt()).isEqualTo(1);
                    assertThat(envelope.path("metadata").path("eventId").asText())
                            .isEqualTo(jdbc.queryForObject("SELECT event_id FROM outbox_event", UUID.class).toString());
                    assertThat(envelope.path("metadata").path("correlationId").asText()).isEqualTo(execution.correlationId().toString());
                    assertThat(envelope.path("payload").path("value").asText()).isEqualTo("safe");
                }
            }
        } finally { broker.destroy(); }
    }

    @Test
    void installedProvisioningReadiesIncomingAndCustomDltBeforeListenersStart() throws Exception {
        String source = "external-incoming-" + UUID.randomUUID();
        var broker = new EmbeddedKafkaKraftBroker(1, 1);
        broker.brokerProperties(Map.of("auto.create.topics.enable", "false"));
        try (var pg = EmbeddedPostgres.builder().start()) {
            broker.afterPropertiesSet();
            String url = pg.getJdbcUrl("postgres", "postgres");
            Flyway.configure().dataSource(url, "postgres", "").locations("classpath:db/platform").load().migrate();
            try (var context = boot("--spring.datasource.url=" + url, "--spring.datasource.username=postgres",
                    "--spring.datasource.password=", "--apex.messaging.cell.enabled=true",
                    "--apex.messaging.outbox.publisher.enabled=false", "--apex.messaging.consumer.enabled=true",
                    "--apex.messaging.consumer.topics=" + source, "--apex.messaging.provisioning.enabled=true",
                    "--apex.messaging.provisioning.partitions=3", "--apex.messaging.consumer.reliability.enabled=true",
                    "--apex.messaging.consumer.reliability.dlt-suffix=.quarantine",
                    "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                    "--spring.kafka.admin.auto-create=false", "--spring.kafka.consumer.group-id=external-provisioning",
                    "--spring.kafka.consumer.enable-auto-commit=false", "--spring.kafka.listener.ack-mode=manual_immediate",
                    "--spring.kafka.consumer.properties.allow.auto.create.topics=false");
                 var admin = org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
                assertThat(context.getBean(com.apex.platform.messaging.jdbc.ReliableKafkaConsumerRuntime.class).isRunning()).isTrue();
                var containers = context.getBean(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class).getListenerContainers();
                assertThat(containers).isNotEmpty().allMatch(org.springframework.kafka.listener.MessageListenerContainer::isRunning);
                var descriptions = admin.describeTopics(List.of(source, source + ".quarantine")).allTopicNames()
                        .get(10, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(descriptions.values()).allSatisfy(description -> {
                    assertThat(description.partitions()).hasSize(3).allSatisfy(partition -> {
                        assertThat(partition.leader().id()).isGreaterThanOrEqualTo(0);
                        assertThat(partition.replicas()).isNotEmpty();
                    });
                });
            }
        } finally { broker.destroy(); }
    }

    @Test
    void installedGrpcUsesPublicJavaQueryLocallyAndAcrossRealHttp2() {
        String noDb = "--spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration";
        try (var server = boot(noDb, "--apex.grpc.server.enabled=true", "--spring.grpc.server.enabled=true",
                "--apex.grpc.cell-id=external-server", "--apex.grpc.insecure-loopback-test-only=true",
                "--apex.grpc.test-clients[0]=external-client", "--spring.grpc.server.address=127.0.0.1",
                "--spring.grpc.server.port=0");
             var client = boot(noDb, "--apex.grpc.client.enabled=true", "--apex.grpc.cell-id=external-client",
                     "--apex.grpc.insecure-loopback-test-only=true", "--apex.grpc.destinations.external-server.channel=server",
                     "--spring.grpc.client.channel.server.target=static://127.0.0.1:" + PORT.get(),
                     "--spring.grpc.client.channel.server.ssl.enabled=false")) {
            assertThat(PORT.get()).isPositive();
            try (var scope = client.getBean(ExecutionContextProvider.class).open(ExecutionContext.root("external-client", "external-client"))) {
                var gateway = client.getBean(QueryGateway.class);
                assertThat(gateway.query("external-server", new Read("remote"), Reply.class)).isEqualTo(new Reply("remote"));
                assertThat(gateway.queryLocal(new Read("local"), Reply.class,
                        com.apex.platform.grpc.api.QueryOptions.defaults())).isEqualTo(new Reply("local"));
            }
        }
    }

    @Test
    void installedDiscoveryRejectsInvalidProviderBeforeAnyRegistryConnection() {
        var policy = new com.apex.platform.discovery.DiscoveryProperties();
        policy.setProvider("eureka,kubernetes");
        assertThatThrownBy(policy::validate)
                .hasMessageContaining("Invalid explicit discovery mode");
    }

    @Test
    void installedDiscoveryReadsARealOwnedRegistryWithoutRegisteringTheConsumer() throws Exception {
        var jar = java.nio.file.Path.of(System.getProperty("external.registry.jar"));
        assertThat(jar).isRegularFile();
        int port;
        try (var socket = new java.net.ServerSocket(0, 0, java.net.InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort();
        }
        var registry = new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-jar", jar.toString(), "--server.address=127.0.0.1", "--server.port=" + port,
                "--apex.registry.loopback-development=true", "--eureka.server.enable-self-preservation=false",
                "--eureka.server.use-read-only-response-cache=false", "--eureka.server.wait-time-in-ms-when-sync-empty=0")
                .redirectErrorStream(true).redirectOutput(java.nio.file.Path.of("target/external-registry.log").toFile()).start();
        try (var http = java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            String origin = "http://127.0.0.1:" + port;
            await().atMost(Duration.ofSeconds(30)).ignoreExceptions().until(() -> {
                assertThat(registry.isAlive()).isTrue();
                return http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(origin + "/actuator/health"))
                        .timeout(Duration.ofSeconds(2)).GET().build(), java.net.http.HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
            });
            // Metadata-only fixture endpoint; no connection to a Business Cell is made.
            String instance = """
                    {"instance":{"instanceId":"external-target:fixture","hostName":"127.0.0.1",
                    "app":"EXTERNAL-TARGET","ipAddr":"127.0.0.1","status":"UP",
                    "port":{"$":12345,"@enabled":"true"},"securePort":{"$":12346,"@enabled":"false"},
                    "dataCenterInfo":{"@class":"com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo","name":"MyOwn"},
                    "vipAddress":"external-target","metadata":{"apex.cell-id":"external-target",
                    "apex.grpc.port":"12346","apex.grpc.authority":"localhost"}}}
                    """;
            var registration = http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(origin + "/eureka/apps/EXTERNAL-TARGET"))
                    .timeout(Duration.ofSeconds(3)).header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(instance)).build(), java.net.http.HttpResponse.BodyHandlers.discarding());
            assertThat(registration.statusCode()).isEqualTo(204);
            try (var context = boot("--spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
                    "--spring.profiles.active=eureka", "--apex.discovery.provider=eureka",
                    "--apex.discovery.insecure-loopback-test-only=true", "--apex.discovery.registration.enabled=false",
                    "--eureka.client.register-with-eureka=false",
                    "--apex.discovery.targets.external-target.authority=localhost",
                    "--eureka.client.service-url.defaultZone=" + origin + "/eureka/", "--eureka.client.registry-fetch-interval-seconds=1")) {
                var directory = context.getBean(com.apex.platform.discovery.CellServiceDirectory.class);
                await().atMost(Duration.ofSeconds(10)).ignoreExceptionsInstanceOf(IllegalStateException.class).untilAsserted(() -> {
                    var endpoints = directory.endpoints("external-target");
                    assertThat(endpoints).hasSize(1);
                    assertThat(endpoints.getFirst().instanceId()).isEqualTo("external-target:fixture");
                    assertThat(endpoints.getFirst().httpPort()).isEqualTo(12345);
                    assertThat(endpoints.getFirst().grpcPort()).isEqualTo(12346);
                    assertThat(endpoints.getFirst().tlsAuthority()).isEqualTo("localhost");
                });
                var unrelated = http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(origin + "/eureka/apps/EXTERNAL-CONSUMER"))
                        .timeout(Duration.ofSeconds(3)).GET().build(), java.net.http.HttpResponse.BodyHandlers.discarding());
                assertThat(unrelated.statusCode()).isEqualTo(404);
                System.out.println("EXTERNAL_DISCOVERY_ACCEPTED registryPid=" + registry.pid() + " provider=eureka endpoints=1 registration=false");
            }
        } finally {
            registry.destroy();
            if (!registry.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) registry.destroyForcibly().waitFor();
            assertThat(registry.isAlive()).isFalse();
        }
    }
}

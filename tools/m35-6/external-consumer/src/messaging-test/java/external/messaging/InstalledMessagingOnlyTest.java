package external.messaging;

import com.apex.platform.core.context.*;
import com.apex.platform.events.api.*;
import com.apex.platform.messaging.consumer.OnCellEvent;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.flywaydb.core.Flyway;
import java.time.Duration;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class InstalledMessagingOnlyTest {
    public record Changed(String value) implements com.apex.platform.events.IntegrationEvent {}
    public static class Handler {
        private final JdbcTemplate jdbc;
        public Handler(JdbcTemplate jdbc){this.jdbc=jdbc;}
        @OnCellEvent(type="INSTALLED_CHANGED",version=1)
        public void changed(Changed payload){jdbc.update("INSERT INTO installed_effect(value) VALUES (?)",payload.value());}
    }
    @Configuration(proxyBeanMethods=false) @EnableAutoConfiguration @Import(Handler.class)
    static class App {}
    @Test void selectedProducerAndConsumerDeliverOneCommittedEffectWithoutOtherTransports() throws Exception {
        for(String type:List.of("io.grpc.ManagedChannel","com.netflix.discovery.EurekaClient","io.fabric8.kubernetes.client.KubernetesClient"))
            assertThat(org.springframework.util.ClassUtils.isPresent(type,getClass().getClassLoader())).as(type).isFalse();
        String topic="installed-messaging-"+UUID.randomUUID();
        var broker=new EmbeddedKafkaKraftBroker(1,1);broker.brokerProperties(Map.of("auto.create.topics.enable","false"));
        try(var pg=EmbeddedPostgres.builder().start()) {
            broker.afterPropertiesSet();String url=pg.getJdbcUrl("postgres","postgres");
            Flyway.configure().dataSource(url,"postgres","").locations("classpath:db/platform").load().migrate();
            try(var connection=pg.getPostgresDatabase().getConnection();var statement=connection.createStatement()){
                statement.execute("CREATE TABLE installed_effect(value text NOT NULL)");
            }
            var app=new SpringApplication(App.class);app.setWebApplicationType(WebApplicationType.NONE);
            try(var context=app.run("--spring.config.location=optional:classpath:/absent.properties",
                    "--spring.application.name=installed-messaging","--apex.cell.id=installed-messaging",
                    "--spring.datasource.url="+url,"--spring.datasource.username=postgres","--spring.datasource.password=",
                    "--apex.messaging.cell.enabled=true","--apex.messaging.provisioning.enabled=true",
                    "--apex.messaging.publisher.topic="+topic,"--apex.messaging.consumer.enabled=true",
                    "--apex.messaging.consumer.topics="+topic,"--apex.messaging.consumer.reliability.enabled=true",
                    "--spring.kafka.bootstrap-servers="+broker.getBrokersAsString(),"--spring.kafka.admin.auto-create=false",
                    "--spring.kafka.consumer.group-id=installed-only","--spring.kafka.consumer.enable-auto-commit=false",
                    "--spring.kafka.consumer.auto-offset-reset=earliest",
                    "--spring.kafka.listener.ack-mode=manual_immediate","--spring.kafka.consumer.properties.allow.auto.create.topics=false",
                    "--spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                    "--spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer")) {
                var jdbc=context.getBean(JdbcTemplate.class);
                try(var scope=context.getBean(ExecutionContextProvider.class).open(ExecutionContext.root("installed-messaging","installed-messaging"))) {
                    new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).executeWithoutResult(status ->
                            context.getBean(CellEvents.class).append(EventContracts.define("INSTALLED_CHANGED",1,"SDK",Changed.class,"installed.v1"),UUID.randomUUID(),new Changed("one")));
                }
                await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                    assertThat(jdbc.queryForObject("SELECT count(*) FROM installed_effect",Integer.class)).isEqualTo(1);
                    assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event",Integer.class)).isEqualTo(1);
                    assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE published",Integer.class)).isEqualTo(1);
                });
            }
        }finally{broker.destroy();}
    }
}

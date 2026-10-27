package com.apex.payment;
import java.util.*;
import java.time.Instant;
import org.junit.jupiter.api.*;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import com.apex.platform.events.*;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

@Tag("integration")
class PaymentIntegrationTest {
    static EmbeddedPostgres db; static EmbeddedKafkaKraftBroker broker;
    static ConfigurableApplicationContext app; static JdbcTemplate jdbc;
    static final JsonMapper JSON=JsonMapper.builder().build();
    static ConfigurableApplicationContext boot(String profile) {
        var env=new StandardEnvironment();
        env.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        env.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        return new SpringApplicationBuilder(PaymentCellApplication.class).environment(env).run(
                "--spring.config.location=classpath:application.yml","--spring.config.import=","--spring.config.additional-location=",
                "--spring.profiles.active="+profile,"--spring.profiles.include=",
                "--spring.datasource.url="+db.getJdbcUrl("postgres","postgres"),"--spring.datasource.username=postgres","--spring.datasource.password=",
                "--spring.kafka.bootstrap-servers="+broker.getBrokersAsString(),"--spring.kafka.consumer.group-id=payment-test",
                "--apex.messaging.outbox.publisher.enabled=false","--spring.kafka.listener.auto-startup=false");
    }
    @BeforeAll static void start() throws Exception {
        try { db=EmbeddedPostgres.builder().start(); broker=new EmbeddedKafkaKraftBroker(1,1,"apex.order.payment-requests.v1","apex.order.payment-requests.v1-dlt");
            broker.brokerProperty("auto.create.topics.enable","false"); broker.brokerListProperty("apex.payment.test.brokers"); broker.afterPropertiesSet();
            app=boot("simulation"); jdbc=app.getBean(JdbcTemplate.class);
        } catch(Exception e) { close(); throw e; }
    }
    @AfterAll static void close() throws Exception {
        try { if(app!=null) app.close(); } finally { try { if(broker!=null) broker.destroy(); } finally { if(db!=null) db.close(); } }
    }
    UUID event(UUID order,String aggregate) {
        UUID id=UUID.randomUUID();
        var envelope=new EventEnvelope<>(new EventMetadata(id,"ORDER_READY_FOR_PAYMENT",1,UUID.randomUUID(),null,order.toString(),aggregate,"order-cell","order-cell",Instant.now()),Map.of("orderId",order));
        app.getBean(ConfigurableKafkaEventConsumer.class).consume(new ConsumerRecord<>("apex.order.payment-requests.v1",0,0,"key",JSON.writeValueAsString(envelope)),()->{});
        return id;
    }
    @Test void approvalAndDeclineAreDurableBusinessDecisions() {
        for(String status:List.of("AUTHORIZED","DECLINED")) {
            UUID id=UUID.randomUUID(); jdbc.update("INSERT INTO payment_simulation_policy VALUES (?,?)",id,status);
            event(id,"ORDER"); event(id,"ORDER");
            assertThat(jdbc.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?",String.class,id)).isEqualTo(status);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=?",Long.class,id)).isEqualTo(1);
        }
    }
    @Test void noPolicyRollsBackInboxClaimAndOutboxThenCanRetry() {
        UUID id=UUID.randomUUID(); long before=jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class);
        assertThatThrownBy(()->event(id,"ORDER")).isInstanceOf(RuntimeException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_authorizations WHERE order_id=?",Long.class,id)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class)).isEqualTo(before);
        jdbc.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')",id); event(id,"ORDER");
    }
    @Test void concurrentBusinessDuplicatesClaimOneAuthorizationAndOutcome() throws Exception {
        UUID id=UUID.randomUUID(); jdbc.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')",id);
        try(var pool=java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var gate=new java.util.concurrent.CountDownLatch(1);
            var first=pool.submit(()->{gate.await();return event(id,"ORDER");});
            var second=pool.submit(()->{gate.await();return event(id,"ORDER");});
            gate.countDown();
            for(UUID eventId:List.of(first.get(15,java.util.concurrent.TimeUnit.SECONDS),second.get(15,java.util.concurrent.TimeUnit.SECONDS))) {
                assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE event_id=?",Long.class,eventId)).isEqualTo(1);
            }
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_authorizations WHERE order_id=?",Long.class,id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=?",Long.class,id)).isEqualTo(1);
    }
    @Test void wrongAggregateFailsBeforeAnyBusinessDecision() {
        UUID id=UUID.randomUUID();
        assertThatThrownBy(()->event(id,"INVENTORY_RESERVATION")).isInstanceOf(IllegalArgumentException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_authorizations WHERE order_id=?",Long.class,id)).isZero();
    }
    @Test void outsideSimulationProfileHasNoImplicitPolicy() {
        assertThatThrownBy(()->boot("")).hasStackTraceContaining("PaymentPolicy");
    }
    @Test void commitFailureRollsBackDecisionInboxAndOutbox() {
        UUID id=UUID.randomUUID(); jdbc.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')",id);
        jdbc.execute("CREATE FUNCTION fail_payment_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit'; END $$");
        jdbc.execute("CREATE CONSTRAINT TRIGGER fail_payment_commit AFTER INSERT ON payment_authorizations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_payment_commit()");
        long before=jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class);
        try { assertThatThrownBy(()->event(id,"ORDER")).isInstanceOf(RuntimeException.class);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_authorizations WHERE order_id=?",Long.class,id)).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=?",Long.class,id)).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class)).isEqualTo(before);
        } finally { jdbc.execute("DROP TRIGGER fail_payment_commit ON payment_authorizations"); jdbc.execute("DROP FUNCTION fail_payment_commit()"); }
        event(id,"ORDER");
    }
}

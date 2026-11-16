package com.apex.platform.messaging.integration;

import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;
import com.apex.platform.events.*;
import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.jdbc.*;
import com.apex.platform.messaging.kafka.SpringKafkaMessagePublisher;
import com.apex.platform.messaging.outbox.*;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.context.annotation.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL commits/races and an acknowledged real Kafka send before simulated process loss. */
class PostgresqlStorageRecoveryTest {
    static EmbeddedPostgres pg;
    static JdbcTemplate jdbc;
    static DataSourceTransactionManager manager;
    static TransactionTemplate tx;
    static AnnotationConfigApplicationContext context;
    static OutboxStore store;
    static JdbcOutboxAppender appender;
    static JdbcProcessedEventStore inbox;
    static final JsonMapper JSON = JsonMapper.builder().build();
    public record Changed(int amount) implements IntegrationEvent {}
    @Configuration(proxyBeanMethods=false) @EnableTransactionManagement
    static class Transactions {}

    @BeforeAll static void start() throws Exception {
        pg=EmbeddedPostgres.builder().start(); var ds=pg.getPostgresDatabase(); jdbc=new JdbcTemplate(ds);
        manager=new DataSourceTransactionManager(ds); tx=new TransactionTemplate(manager);
        new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),
                new ClassPathResource("db/platform/V1002__platform_processed_event.sql")).execute(ds);
        jdbc.execute("CREATE TABLE aor_effect(id uuid PRIMARY KEY)");
        context=new AnnotationConfigApplicationContext(); context.register(Transactions.class);
        context.registerBean("transactionManager", DataSourceTransactionManager.class, () -> manager);
        context.registerBean("store", OutboxStore.class, () -> new JdbcOutboxStore(jdbc, Clock.systemUTC()));
        context.registerBean("executor", EventHandlerExecutor.class, () -> new SimpleTransactionalEventHandlerExecutor(
                new JdbcProcessedEventStore(jdbc, Clock.systemUTC()), new ThreadLocalExecutionContextProvider(), "test", "test"));
        context.refresh();store=context.getBean(OutboxStore.class);
        appender=new JdbcOutboxAppender(jdbc,JSON);inbox=new JdbcProcessedEventStore(jdbc,Clock.systemUTC());
        assertThat(org.springframework.aop.support.AopUtils.isAopProxy(store)).isTrue();
    }
    @AfterAll static void close() throws Exception {if(context!=null)context.close();if(pg!=null)pg.close();}
    @BeforeEach void clear() {jdbc.update("DELETE FROM outbox_event");jdbc.update("DELETE FROM processed_event");jdbc.update("DELETE FROM aor_effect");}
    EventEnvelope<Changed> event() {
        UUID id=UUID.randomUUID();return new EventEnvelope<>(new EventMetadata(id,"AOR_CHANGED",1,id,null,id.toString(),
                "PROBE","test","test",Instant.now()),new Changed(1));
    }
    void append(EventEnvelope<Changed> event) {tx.executeWithoutResult(s -> appender.append(event));}
    EventHandler<Changed> handler(java.util.function.Consumer<EventEnvelope<Changed>> work) {
        return new EventHandler<>() {
            public String eventType(){return "AOR_CHANGED";}
            public int eventVersion(){return 1;}
            public Class<Changed> payloadType(){return Changed.class;}
            public void handle(EventEnvelope<Changed> event,ConsumerContext delivery){work.accept(event);}
        };
    }

    @Test void businessAndOutboxRollbackTogetherAndProxyClaimsCommit() {
        var event=event();
        tx.executeWithoutResult(s -> {jdbc.update("INSERT INTO aor_effect VALUES (?)",event.metadata().eventId());appender.append(event);s.setRollbackOnly();});
        assertThat(jdbc.queryForObject("SELECT count(*) FROM aor_effect",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event",Integer.class)).isZero();
        append(event);var claimed=store.claimBatch(1,"first",Duration.ofMinutes(1));
        assertThat(claimed).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT claim_token FROM outbox_event",UUID.class)).isEqualTo(claimed.getFirst().claimToken());
    }

    @Test void concurrentPublishersBoundClaimsAndRejectStaleFencesThenRecoverAndRetry() throws Exception {
        append(event());append(event());
        try(var pool=Executors.newFixedThreadPool(2)) {
            var gate=new CountDownLatch(1);
            var first=pool.submit(() -> {gate.await();return store.claimBatch(1,"first",Duration.ofMinutes(1));});
            var second=pool.submit(() -> {gate.await();return store.claimBatch(1,"second",Duration.ofMinutes(1));});gate.countDown();
            var a=first.get(10,TimeUnit.SECONDS).getFirst();var b=second.get(10,TimeUnit.SECONDS).getFirst();
            assertThat(a.id()).isNotEqualTo(b.id());assertThat(store.claimBatch(1,"third",Duration.ofSeconds(10))).isEmpty();
            jdbc.update("UPDATE outbox_event SET claimed_until=now()-interval '1 second' WHERE id=?",a.id());
            var reclaimed=store.claimBatch(1,"third",Duration.ofMinutes(1)).getFirst();
            assertThat(reclaimed.id()).isEqualTo(a.id());assertThat(reclaimed.claimToken()).isNotEqualTo(a.claimToken());
            assertThat(store.markPublished(a.id(),a.claimToken())).isFalse();
            assertThat(store.markFailed(a.id(),a.claimToken(),"stale",Instant.now())).isFalse();
            assertThat(store.moveToDeadLetter(a.id(),a.claimToken(),"stale")).isFalse();
            assertThat(store.markFailed(a.id(),reclaimed.claimToken(),"retry",Instant.now().plusSeconds(60))).isTrue();
            assertThat(store.claimBatch(1,"fourth",Duration.ofSeconds(10))).isEmpty();
            jdbc.update("UPDATE outbox_event SET next_retry_at=now()-interval '1 second' WHERE id=?",a.id());
            var retry=store.claimBatch(1,"fourth",Duration.ofSeconds(10)).getFirst();
            assertThat(retry.retryCount()).isEqualTo(1);assertThat(store.moveToDeadLetter(a.id(),retry.claimToken(),"exhausted")).isTrue();
            assertThat(store.markPublished(b.id(),b.claimToken())).isTrue();
            assertThat(store.claimBatch(10,"fifth",Duration.ofSeconds(10))).isEmpty();
        }
    }

    @Test void concurrentInboxDuplicatesAndHandlerRollbackUseTheOwnerTransaction() throws Exception {
        var event=event();var delivery=new ConsumerContext("aor-group","events",0,1,"key",Instant.now());
        EventHandler<Changed> handler=handler(e -> jdbc.update("INSERT INTO aor_effect VALUES (?)",e.metadata().eventId()));
        var executor=context.getBean(EventHandlerExecutor.class);
        assertThatThrownBy(() -> executor.execute(handler(e -> {handler.handle(e,delivery);throw new IllegalStateException("rollback");}),event,delivery))
                .hasMessageContaining("rollback");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM aor_effect",Integer.class)).isZero();
        try(var pool=Executors.newFixedThreadPool(2)) {
            var gate=new CountDownLatch(1);
            Callable<DispatchResult> call=() -> {gate.await();return executor.execute(handler,event,delivery);};
            var first=pool.submit(call);var second=pool.submit(call);gate.countDown();
            assertThat(List.of(first.get(10,TimeUnit.SECONDS),second.get(10,TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(DispatchResult.HANDLED,DispatchResult.DUPLICATE);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM aor_effect",Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event",Integer.class)).isEqualTo(1);
    }

    @Test void acknowledgedKafkaSendBeforeProcessLossIsRedeliveredAfterLeaseRecovery() throws Exception {
        String topic="aor-recovery-"+UUID.randomUUID();
        var broker=new EmbeddedKafkaKraftBroker(1,1,topic);broker.afterPropertiesSet();
        var factory=new DefaultKafkaProducerFactory<String,String>(Map.of("bootstrap.servers",broker.getBrokersAsString(),
                "key.serializer",StringSerializer.class,"value.serializer",StringSerializer.class,"acks","all"));
        var kafka=new KafkaTemplate<>(factory);
        try {
            var event=event();append(event);
            OutboxStore crashAfterAck=new OutboxStore() {
                public List<OutboxRecord> findBatch(int size){return store.findBatch(size);}
                public List<OutboxRecord> claimBatch(int size,String instance,Duration lease){return store.claimBatch(size,instance,lease);}
                public boolean markPublished(Long id,UUID token){throw new AssertionError("simulated process loss after real broker ACK");}
                public boolean markFailed(Long id,UUID token,String error,Instant next){return store.markFailed(id,token,error,next);}
                public boolean moveToDeadLetter(Long id,UUID token,String error){return store.moveToDeadLetter(id,token,error);}
            };
            var transport=new SpringKafkaMessagePublisher(kafka);
            var first=new ConfigurableOutboxPublisher(crashAfterAck,transport,JSON,new JacksonOutboxEventMapper(JSON),new RetryPolicy(),"lost",topic);
            assertThatThrownBy(first::publishBatch).isInstanceOf(AssertionError.class).hasMessageContaining("after real broker ACK");
            assertThat(jdbc.queryForObject("SELECT published FROM outbox_event",Boolean.class)).isFalse();
            jdbc.update("UPDATE outbox_event SET claimed_until=now()-interval '1 second'");
            new ConfigurableOutboxPublisher(store,transport,JSON,new JacksonOutboxEventMapper(JSON),new RetryPolicy(),"restarted",topic).publishBatch();
            assertThat(jdbc.queryForObject("SELECT published FROM outbox_event",Boolean.class)).isTrue();
            var props=new Properties();props.put("bootstrap.servers",broker.getBrokersAsString());props.put("group.id",UUID.randomUUID().toString());
            props.put("auto.offset.reset","earliest");props.put("enable.auto.commit","false");
            try(var consumer=new KafkaConsumer<String,String>(props,new StringDeserializer(),new StringDeserializer())) {
                consumer.subscribe(List.of(topic));var received=new ArrayList<ConsumerRecord<String,String>>();
                long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
                while(received.size()<2 && System.nanoTime()<deadline) consumer.poll(Duration.ofMillis(250)).forEach(received::add);
                assertThat(received).hasSize(2);assertThat(received.get(0).value()).isEqualTo(received.get(1).value());
                assertThat(received.get(0).value()).contains(event.metadata().eventId().toString());
            }
        } finally {kafka.destroy();factory.destroy();broker.destroy();}
    }
}

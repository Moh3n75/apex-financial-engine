package com.apex.platform.messaging.integration;

import static org.assertj.core.api.Assertions.*;
import com.apex.platform.events.*;
import com.apex.platform.messaging.jdbc.*;
import com.apex.platform.messaging.observation.MessagingObservation;
import io.micrometer.observation.*;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** Real owning PostgreSQL commit evidence, including deferred COMMIT failure. */
class ObservationTransactionTest {
    static EmbeddedPostgres pg;static JdbcTemplate jdbc;static TransactionTemplate tx;
    final List<String> seen=new ArrayList<>();
    MessagingObservation observations;JdbcOutboxAppender append;JdbcOutboxStore store;
    public record Changed(int quantity) implements IntegrationEvent {}
    @BeforeAll static void database() throws Exception{
        pg=EmbeddedPostgres.builder().start();var dataSource=pg.getPostgresDatabase();
        jdbc=new JdbcTemplate(dataSource);
        tx=new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql")).execute(dataSource);
        jdbc.execute("CREATE TABLE ops_fault(enabled boolean NOT NULL)");jdbc.execute("INSERT INTO ops_fault VALUES(false)");
        jdbc.execute("CREATE FUNCTION ops_fail_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.published AND (SELECT enabled FROM ops_fault) THEN RAISE EXCEPTION 'isolated mark commit failure'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE CONSTRAINT TRIGGER ops_fail_mark AFTER UPDATE ON outbox_event DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ops_fail_mark()");
    }
    @AfterAll static void cleanup() throws Exception{if(pg!=null)pg.close();}
    @BeforeEach void prepare(){
        jdbc.update("UPDATE ops_fault SET enabled=false");jdbc.update("DELETE FROM outbox_event");
        var registry=ObservationRegistry.create();registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>(){
            public boolean supportsContext(Observation.Context context){return true;}
            public void onStop(Observation.Context context){seen.add(context.getName()+":"+context.getLowCardinalityKeyValue("apex.outcome").getValue());}
        });observations=new MessagingObservation(registry);append=new JdbcOutboxAppender(jdbc,JsonMapper.builder().build(),observations);
        store=new JdbcOutboxStore(jdbc,Clock.systemUTC(),observations);
    }
    EventEnvelope<Changed> event(){var id=UUID.randomUUID();return new EventEnvelope<>(new EventMetadata(id,"CHANGED",1,id,null,
        id.toString(),"PROBE","test","test",Instant.now()),new Changed(1));}
    @Test void committedHintOnlyAfterOuterCommitAndRollbackIsNotCommit(){
        tx.executeWithoutResult(status->{append.append(event());assertThat(seen).contains("apex.outbox.intent:OBSERVED").doesNotContain("apex.outbox.intent:COMMITTED");});
        assertThat(seen).contains("apex.outbox.intent:COMMITTED");seen.clear();
        tx.executeWithoutResult(status->{append.append(event());status.setRollbackOnly();});
        assertThat(seen).contains("apex.outbox.intent:ROLLED_BACK").doesNotContain("apex.outbox.intent:COMMITTED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event",Integer.class)).isEqualTo(1);
    }
    @Test void falseMarkCannotProduceCommittedEvidence(){
        tx.executeWithoutResult(status->append.append(event()));
        var records=tx.execute(status->store.claimBatch(1,"publisher",Duration.ofMinutes(1)));seen.clear();
        Boolean marked=tx.execute(status->store.markPublished(records.getFirst().id(),UUID.randomUUID()));
        assertThat(marked).isFalse();
        assertThat(seen).contains("apex.outbox.mark:UNKNOWN").doesNotContain("apex.outbox.mark:COMMITTED");
        assertThat(jdbc.queryForObject("SELECT published FROM outbox_event",Boolean.class)).isFalse();
    }
    @Test void successfulMarkIsOnlyConfirmedAfterCommit(){
        tx.executeWithoutResult(status->append.append(event()));
        var record=tx.execute(status->store.claimBatch(1,"publisher",Duration.ofMinutes(1))).getFirst();seen.clear();
        tx.executeWithoutResult(status->{assertThat(store.markPublished(record.id(),record.claimToken())).isTrue();
            assertThat(seen).doesNotContain("apex.outbox.mark:COMMITTED");});
        assertThat(seen).containsExactly("apex.outbox.mark:COMMITTED");
        assertThat(jdbc.queryForObject("SELECT published FROM outbox_event",Boolean.class)).isTrue();
    }
    @Test void deferredMarkCommitFailureRollsBackAndNeverPublishesCommittedHint(){
        tx.executeWithoutResult(status->append.append(event()));
        var record=tx.execute(status->store.claimBatch(1,"publisher",Duration.ofMinutes(1))).getFirst();seen.clear();
        jdbc.update("UPDATE ops_fault SET enabled=true");
        assertThatThrownBy(()->tx.executeWithoutResult(status->{assertThat(store.markPublished(record.id(),record.claimToken())).isTrue();
            assertThat(seen).doesNotContain("apex.outbox.mark:COMMITTED");})).isInstanceOf(RuntimeException.class);
        // Transaction manager reports a failed commit; JDBC rollback/unknown is never a positive mark.
        assertThat(seen).doesNotContain("apex.outbox.mark:COMMITTED");
        assertThat(jdbc.queryForObject("SELECT published FROM outbox_event",Boolean.class)).isFalse();
    }
    @Test void observationHandlerExceptionCannotFailOwnerCommit(){
        var registry=ObservationRegistry.create();registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>(){
            public boolean supportsContext(Observation.Context c){return true;}
            public void onStart(Observation.Context c){throw new IllegalStateException("CANARY");}
        });var appender=new JdbcOutboxAppender(jdbc,JsonMapper.builder().build(),new MessagingObservation(registry));
        assertThatCode(()->tx.executeWithoutResult(status->appender.append(event()))).doesNotThrowAnyException();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event",Integer.class)).isEqualTo(1);
    }
}

package com.apex.operations.owner;
import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.*;import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;import java.util.*;import javax.sql.DataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;import org.springframework.core.io.ClassPathResource;
class MessagingEvidenceReaderTest {
 static EmbeddedPostgres pg;static DataSource ds;
 final MessagingEvidenceReader reader=new MessagingEvidenceReader(Set.of("known"));
 @BeforeAll static void start()throws Exception {pg=EmbeddedPostgres.builder().start();ds=pg.getPostgresDatabase();
 new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),new ClassPathResource("db/platform/V1002__platform_processed_event.sql")).execute(ds);}
 @AfterAll static void stop()throws Exception {if(pg!=null)pg.close();}
 Connection writer()throws Exception{var c=ds.getConnection();c.setAutoCommit(false);return c;}
 void insert(Connection c,UUID id)throws Exception{try(var s=c.prepareStatement("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload) VALUES (?,'TEST',1,?,?, 'TEST','test','test',now(),'{\"password\":\"CANARY_SECRET\"}')")){s.setObject(1,id);s.setObject(2,id);s.setObject(3,id);s.executeUpdate();}}
 List<com.apex.operations.contracts.OwnerSnapshot.Fact> read(UUID id)throws Exception{try(var c=ds.getConnection()){c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);return reader.read(c,List.of(id));}}
 String state(UUID id)throws Exception{return read(id).getFirst().state();}
 @Test void uncommittedAndRolledBackAreUnknown()throws Exception {var id=UUID.randomUUID();try(var c=writer()){insert(c,id);assertThat(state(id)).isEqualTo("UNKNOWN");c.rollback();}assertThat(state(id)).isEqualTo("UNKNOWN");}
 @Test void lateOlderCommitFoundByExactReread()throws Exception {var a=UUID.randomUUID();var b=UUID.randomUUID();try(var ca=writer();var cb=writer()){insert(ca,a);insert(cb,b);cb.commit();assertThat(state(a)).isEqualTo("UNKNOWN");assertThat(state(b)).isEqualTo("PENDING");ca.commit();}assertThat(state(a)).isEqualTo("PENDING");}
 @Test void publicationMarkRequiresCommittedUpdateAndNeverInfersAck()throws Exception {var id=UUID.randomUUID();try(var c=writer()){insert(c,id);c.commit();}
 assertThat(state(id)).isEqualTo("PENDING");try(var c=writer();var q=c.prepareStatement("UPDATE outbox_event SET published=true WHERE event_id=?")){q.setObject(1,id);q.executeUpdate();assertThat(state(id)).isEqualTo("PENDING");c.rollback();}assertThat(state(id)).isEqualTo("PENDING");
 try(var c=writer();var q=c.prepareStatement("UPDATE outbox_event SET published=true WHERE event_id=?")){q.setObject(1,id);q.executeUpdate();c.commit();}assertThat(state(id)).isEqualTo("PUBLISHED_MARK");}
 @Test void knownInboxCommitDistinctFromUnknownOtherConsumers()throws Exception {var id=UUID.randomUUID();try(var c=writer();var q=c.prepareStatement("INSERT INTO processed_event VALUES ('known',?,'TEST',1,'test',0,1,now())")){q.setObject(1,id);q.executeUpdate();assertThat(read(id).get(1).state()).isEqualTo("UNKNOWN");c.commit();}
 assertThat(read(id).get(1).state()).isEqualTo("COMMITTED");assertThat(read(id)).noneMatch(f->f.scope().equals("unknown"));}
 @Test void duplicateSnapshotsIdempotentAndPayloadNeverSelected()throws Exception {var id=UUID.randomUUID();try(var c=writer()){insert(c,id);c.commit();}assertThat(read(id)).isEqualTo(read(id));assertThat(read(id).toString()).doesNotContain("CANARY_SECRET","password");}
 @Test void readPolicyAndQueryBoundsEnforced()throws Exception {try(var c=ds.getConnection()){assertThatThrownBy(()->reader.read(c,List.of(UUID.randomUUID()))).isInstanceOf(IllegalArgumentException.class);}assertThatThrownBy(()->new MessagingEvidenceReader(Set.of("secret group"))).isInstanceOf(IllegalArgumentException.class);}
}

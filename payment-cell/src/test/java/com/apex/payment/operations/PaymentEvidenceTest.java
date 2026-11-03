package com.apex.payment.operations;
import static org.assertj.core.api.Assertions.*;import static org.mockito.Mockito.*;
import org.junit.jupiter.api.*;import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import com.apex.operations.contracts.*;import java.sql.*;import java.util.*;import com.zaxxer.hikari.*;
import org.springframework.mock.env.MockEnvironment;import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.availability.*;import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;import org.springframework.core.io.ClassPathResource;
class PaymentEvidenceTest {
 static EmbeddedPostgres pg;static PaymentEvidenceDataSource.Pool pool;
 static MockEnvironment env;static ApplicationAvailability availability;
 @BeforeAll static void start()throws Exception {pg=EmbeddedPostgres.builder().start();
 new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),new ClassPathResource("db/platform/V1002__platform_processed_event.sql"),new ClassPathResource("db/migration/V2000__payment_simulation.sql")).execute(pg.getPostgresDatabase());
 var c=new HikariConfig();c.setJdbcUrl(pg.getJdbcUrl("postgres","postgres"));c.setUsername("postgres");c.setReadOnly(true);c.setMaximumPoolSize(1);c.setConnectionTimeout(300);pool=new PaymentEvidenceDataSource.Pool(new HikariDataSource(c));
 env=new MockEnvironment().withProperty("apex.grpc.security.enabled","true").withProperty("apex.ops03.environment","test").withProperty("apex.ops03.instance","one");
 availability=mock(ApplicationAvailability.class);when(availability.getReadinessState()).thenReturn(ReadinessState.ACCEPTING_TRAFFIC);
 }
 @AfterAll static void close()throws Exception {if(pool!=null)pool.close();if(pg!=null)pg.close();}
 PaymentEvidenceHandler handler(){return new PaymentEvidenceHandler(pool,env,new DefaultListableBeanFactory().getBeanProvider(com.apex.observability.ActiveOperations.class),availability);}
 String state(UUID id)throws Exception {return handler().read(new EvidenceQuery(List.of(),List.of(id))).facts().stream().filter(f->f.kind()==OwnerSnapshot.Kind.WORKFLOW).findFirst().orElseThrow().state();}
 void insert(Connection c,UUID id,String state)throws Exception {try(var q=c.prepareStatement("INSERT INTO payment_authorizations(order_id,status,created_at) VALUES (?,?,now())")){q.setObject(1,id);q.setString(2,state);q.executeUpdate();}}
 @Test void uncommittedRollbackAndLateEarlierCommitAreNotFabricated()throws Exception {
 var a=UUID.randomUUID();var b=UUID.randomUUID();try(var ca=pg.getPostgresDatabase().getConnection();var cb=pg.getPostgresDatabase().getConnection()){ca.setAutoCommit(false);cb.setAutoCommit(false);insert(ca,a,"PENDING");insert(cb,b,"DECLINED");cb.commit();assertThat(state(a)).isEqualTo("UNKNOWN");assertThat(state(b)).isEqualTo("DECLINED");ca.commit();assertThat(state(a)).isEqualTo("PENDING");}
 var rolled=UUID.randomUUID();try(var c=pg.getPostgresDatabase().getConnection()){c.setAutoCommit(false);insert(c,rolled,"PENDING");c.rollback();}assertThat(state(rolled)).isEqualTo("UNKNOWN");
 }
 @Test void allRealOwnerStatesAndReadinessRemainSeparate()throws Exception {for(String value:List.of("PENDING","AUTHORIZED","DECLINED")){var id=UUID.randomUUID();try(var c=pg.getPostgresDatabase().getConnection()){insert(c,id,value);}assertThat(state(id)).isEqualTo(value);}
 var snapshot=handler().read(new EvidenceQuery(List.of(),List.of()));assertThat(snapshot.facts()).anyMatch(f->f.scope().equals("application-readiness")&&f.state().equals("ACCEPTING_TRAFFIC")).anyMatch(f->f.scope().equals("active-coverage")&&f.state().equals("NOT_INSTRUMENTED"));assertThat(snapshot.activeAvailable()).isFalse();
 var h=handler();var query=new EvidenceQuery(List.of(),List.of(UUID.randomUUID()));
 for(int n=0;n<20;n++)h.read(query);
 var bean=(com.sun.management.ThreadMXBean)java.lang.management.ManagementFactory.getThreadMXBean();long tid=Thread.currentThread().threadId();long cpu=bean.getCurrentThreadCpuTime(),bytes=bean.getThreadAllocatedBytes(tid);long[] times=new long[200];
 for(int n=0;n<times.length;n++){long t=System.nanoTime();assertThat(h.read(query).facts()).anyMatch(f->f.kind()==OwnerSnapshot.Kind.WORKFLOW&&f.state().equals("UNKNOWN"));times[n]=System.nanoTime()-t;}
 cpu=bean.getCurrentThreadCpuTime()-cpu;bytes=bean.getThreadAllocatedBytes(tid)-bytes;Arrays.sort(times);
 java.nio.file.Files.writeString(java.nio.file.Path.of("target/ops03b-owner-cost.csv"),"samples,p50Micros,p95Micros,p99Micros,currentThreadCpuNanos,currentThreadAllocatedBytes\n200,"+times[99]/1000+","+times[189]/1000+","+times[197]/1000+","+cpu+","+bytes+"\n");

 }
 @Test void slowSqlAndExhaustedPoolShedAndReleaseAdmission()throws Exception {
 var h=handler();var q=new EvidenceQuery(List.of(),List.of(UUID.randomUUID()));
 var executor=java.util.concurrent.Executors.newSingleThreadExecutor();
 try(var lock=pg.getPostgresDatabase().getConnection()){lock.setAutoCommit(false);lock.createStatement().execute("LOCK TABLE payment_authorizations IN ACCESS EXCLUSIVE MODE");
 var blocked=executor.submit(()->h.read(q));
 org.awaitility.Awaitility.await().pollInterval(java.time.Duration.ofMillis(10)).atMost(java.time.Duration.ofSeconds(1)).until(()->{try(var probe=pg.getPostgresDatabase().getConnection();var statement=probe.createStatement();var result=statement.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT status,created_at FROM payment_authorizations%'")){result.next();return result.getInt(1)>0;}});
 assertThatThrownBy(()->h.read(q)).isInstanceOfSatisfying(com.apex.platform.grpc.api.GatewayException.class,e->assertThat(e.code()).isEqualTo(com.apex.platform.grpc.api.GatewayException.Code.RESOURCE_EXHAUSTED));
 assertThatThrownBy(()->blocked.get(3,java.util.concurrent.TimeUnit.SECONDS)).hasCauseInstanceOf(SQLException.class);lock.rollback();
 }finally{executor.shutdownNow();}
 try(var held=pool.dataSource().getConnection()){assertThatThrownBy(()->h.read(q)).isInstanceOf(SQLException.class);}
 assertThat(h.read(q).facts()).anyMatch(f->f.kind()==OwnerSnapshot.Kind.WORKFLOW&&f.state().equals("UNKNOWN"));
 }
 @Test void insecureOwnerConfigurationIsRejected(){var bad=new MockEnvironment().withProperty("apex.grpc.security.enabled","false");assertThatThrownBy(()->new PaymentEvidenceHandler(pool,bad,new DefaultListableBeanFactory().getBeanProvider(com.apex.observability.ActiveOperations.class),availability)).isInstanceOf(IllegalStateException.class);}
}

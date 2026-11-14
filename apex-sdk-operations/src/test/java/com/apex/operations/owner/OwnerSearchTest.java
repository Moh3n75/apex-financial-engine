package com.apex.operations.owner;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.OwnerSearch.*;
import com.apex.platform.grpc.api.GatewayException;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.file.*;
import javax.sql.DataSource;
import org.junit.jupiter.api.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class OwnerSearchTest {
    static EmbeddedPostgres pg;
    static DataSource db;
    static final Instant BASE=Instant.now();
    static class Time extends Clock {
        Instant now=BASE;public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId z){return this;}public Instant instant(){return now;}
    }
    final Time clock=new Time();final AtomicLong nanos=new AtomicLong();
    @BeforeAll static void start() throws Exception {
        pg=EmbeddedPostgres.builder().start();db=OperationsTestDatabase.pooled(pg.getPostgresDatabase());
        new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),new ClassPathResource("db/platform/V1002__platform_processed_event.sql")).execute(db);
    }
    @AfterAll static void stop() throws Exception {if(db instanceof com.zaxxer.hikari.HikariDataSource pool)pool.close();if(pg!=null)pg.close();}
    @BeforeEach void clear() throws Exception {try(var c=db.getConnection();var s=c.createStatement()){s.execute("TRUNCATE outbox_event,processed_event RESTART IDENTITY");}}
    Query query(Kind kind,String scope,int limit) {return new Query("test","owner",kind,scope,BASE.minusSeconds(1200),BASE.plusSeconds(1),"ANY",limit,"");}
    Query cursor(Query q,String cursor) {return new Query(q.environment(),q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),q.state(),q.limit(),cursor);}
    BoundedOwnerSearch reader(DataSource database,BoundedOwnerSearch.Source source) {
        return new BoundedOwnerSearch(database,"test","owner","one",Map.of(Kind.OUTBOX_EVENT,Set.of("owner"),Kind.KNOWN_GROUP_INBOX,Set.of("known")),
                Map.of(Kind.OUTBOX_EVENT,MessagingSearchSource.OUTBOX_STATES,Kind.KNOWN_GROUP_INBOX,Set.of("HANDLER_TRANSACTION_COMMITTED")),source,clock,()->nanos.addAndGet(2_000_000_000L));
    }
    BoundedOwnerSearch reader(){return reader(db,MessagingSearchSource::read);}
    @Test void slowConnectionAcquisitionStillExhaustsTheProductionReadBudget() {
        assertThatThrownBy(() -> reader(OperationsTestDatabase.slowAcquisition(db),MessagingSearchSource::read)
                .read(query(Kind.OUTBOX_EVENT,"owner",16)))
                .isInstanceOfSatisfying(GatewayException.class,e -> assertThat(e.code()).isEqualTo(GatewayException.Code.DEADLINE_EXCEEDED));
    }
    UUID insert(Connection c,Instant time) throws Exception {
        UUID id=UUID.randomUUID();
        try(var s=c.prepareStatement("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload,created_at) VALUES (?,'TEST',1,?,?,'TEST','owner','owner',?,'{\"secret\":\"CANARY_PAYLOAD\"}',?)")) {
            s.setObject(1,id);s.setObject(2,UUID.randomUUID());s.setObject(3,UUID.randomUUID());s.setObject(4,time.atOffset(ZoneOffset.UTC));s.setObject(5,time.atOffset(ZoneOffset.UTC));s.executeUpdate();
        }return id;
    }
    @Test void earlierUncommittedRowIsMissedByCursorAndConvergesOnlyOnRestartedWalk() throws Exception {
        var h=reader();var q=query(Kind.OUTBOX_EVENT,"owner",1);
        try(var a=db.getConnection();var b=db.getConnection()) {
            a.setAutoCommit(false);UUID late=insert(a,BASE.minusSeconds(30));UUID early=insert(b,BASE.minusSeconds(20));
            var first=h.read(q);assertThat(first.items()).extracting(Item::ref).containsExactly(early);
            a.commit();var continued=h.read(cursor(q,first.nextCursor()));assertThat(continued.items()).isEmpty();
            var restarted=h.read(q);assertThat(restarted.items()).extracting(Item::ref).containsExactly(late);
            assertThat(restarted.coverage()).contains(Gap.PARTIAL_CURRENT_STATE,Gap.RECONCILE_FROM_START,Gap.RETENTION_UNQUALIFIED);
            a.setAutoCommit(false);UUID rollback=insert(a,BASE);a.rollback();
            assertThat(h.read(query(Kind.OUTBOX_EVENT,"owner",16)).items()).extracting(Item::ref).doesNotContain(rollback);
        }
    }
    @Test void cursorRejectsTamperingFiltersLimitsOwnerScopeExpiryAndNewBoot() throws Exception {
        var signer=new SearchCursor(clock);var q=query(Kind.OUTBOX_EVENT,"owner",1);
        String valid=signer.next(q,signer.read(q),"7");assertThat(signer.read(cursor(q,valid)).key()).isEqualTo("7");
        String tampered=valid.substring(0,10)+(valid.charAt(10)=='A'?'B':'A')+valid.substring(11);
        assertThat(tampered).isNotEqualTo(valid);
        assertThatThrownBy(()->signer.read(cursor(q,tampered))).isInstanceOf(IllegalArgumentException.class);
        for(var changed:List.of(query(Kind.OUTBOX_EVENT,"other",1),query(Kind.OUTBOX_EVENT,"owner",2),query(Kind.KNOWN_GROUP_INBOX,"known",1),
                new Query("other","owner",q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),q.state(),q.limit(),""),
                new Query("test","other",q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),q.state(),q.limit(),""),
                new Query("test","owner",q.kind(),q.scope(),q.fromInclusive().plusSeconds(1),q.toExclusive(),q.state(),q.limit(),""),
                new Query("test","owner",q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),"OUTBOX_COMMITTED",q.limit(),"")))
            assertThatThrownBy(()->signer.read(cursor(changed,valid))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->new SearchCursor(clock).read(cursor(q,valid))).isInstanceOf(IllegalArgumentException.class);
        clock.now=BASE.plusSeconds(31);assertThatThrownBy(()->signer.read(cursor(q,valid))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void oldCandidatesConsumeFinitePagesWithoutPretendingRecentAbsence() throws Exception {
        try(var c=db.getConnection()){for(int n=0;n<10;n++)insert(c,BASE.minusSeconds(1800));insert(c,BASE);}
        var h=reader();var q=query(Kind.OUTBOX_EVENT,"owner",1);Page page=null;
        for(int n=1;n<=8;n++){page=h.read(q);assertThat(page.items()).isEmpty();assertThat(page.candidatesExamined()).isEqualTo(1);if(n<8)q=cursor(q,page.nextCursor());}
        assertThat(page.coverage()).contains(Gap.WALK_LIMIT,Gap.CANDIDATE_PAGE_LIMIT);assertThat(page.nextCursor()).isEmpty();
    }
    @Test void groupVisibilityDuplicateSuppressionAndZeroUuidStayScoped() throws Exception {
        try(var c=db.getConnection();var s=c.createStatement()) {
            s.execute("INSERT INTO processed_event VALUES ('known','00000000-0000-0000-0000-000000000000','TEST',1,'test',0,0,TIMESTAMPTZ '"+BASE.minusSeconds(1)+"'),('unknown','00000000-0000-0000-0000-000000000001','TEST',1,'test',0,0,TIMESTAMPTZ '"+BASE.minusSeconds(1)+"')");
            assertThatThrownBy(()->s.execute("INSERT INTO processed_event SELECT * FROM processed_event WHERE consumer_group='known'")).isInstanceOf(SQLException.class);
        }
        var h=reader();assertThat(h.read(query(Kind.KNOWN_GROUP_INBOX,"known",16)).items()).extracting(Item::ref).containsExactly(new UUID(0,0));
        assertThatThrownBy(()->h.read(query(Kind.KNOWN_GROUP_INBOX,"unknown",16))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
        try(var c=db.getConnection();var s=c.createStatement()){s.execute("DELETE FROM processed_event WHERE consumer_group='known'");}
        assertThat(h.read(query(Kind.KNOWN_GROUP_INBOX,"known",16)).coverage()).contains(Gap.RETENTION_UNQUALIFIED);
    }
    @Test void revokedDatabasePermissionIsNotAnEmptySuccessfulPage() throws Exception {
        try(var c=db.getConnection();var s=c.createStatement()){s.execute("CREATE ROLE ops03c_reader LOGIN");s.execute("GRANT USAGE ON SCHEMA public TO ops03c_reader");s.execute("GRANT SELECT ON outbox_event TO ops03c_reader");}
        var restricted=new DriverManagerDataSource(pg.getJdbcUrl("ops03c_reader","postgres"),"ops03c_reader","");
        try(var c=restricted.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT current_user")){r.next();assertThat(r.getString(1)).isEqualTo("ops03c_reader");}
        var h=reader(restricted,MessagingSearchSource::read);
        assertThat(h.read(query(Kind.OUTBOX_EVENT,"owner",16)).items()).isEmpty();
        try(var c=db.getConnection();var s=c.createStatement()){s.execute("REVOKE SELECT ON outbox_event FROM ops03c_reader");}
        assertThatThrownBy(()->h.read(query(Kind.OUTBOX_EVENT,"owner",16))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
    }
    @Test void jdbcUrlUserOverridesPropertiesAndRestrictedFixtureMustVerifyEffectiveRole() throws Exception {
        try(var c=db.getConnection();var s=c.createStatement()){s.execute("CREATE ROLE ops03c_probe LOGIN");}
        for(boolean correct:List.of(false,true)) {
            var candidate=new DriverManagerDataSource(pg.getJdbcUrl(correct?"ops03c_probe":"postgres","postgres"),"ops03c_probe","");
            try(var c=candidate.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT current_user")){r.next();assertThat(r.getString(1)).isEqualTo(correct?"ops03c_probe":"postgres");}
        }
    }
    @Test void lockedSqlShedsConcurrentSearchAndRecoversWithoutWrites() throws Exception {
        var entered=new CountDownLatch(1);var proceed=new CountDownLatch(1);
        var h=reader(db,(c,request,key)->{entered.countDown();try {if(!proceed.await(1,TimeUnit.SECONDS))throw new SQLException("TEST_GATE");}catch(InterruptedException e){Thread.currentThread().interrupt();throw new SQLException("TEST_GATE");}return MessagingSearchSource.read(c,request,key);});
        var q=query(Kind.OUTBOX_EVENT,"owner",16);var workers=Executors.newSingleThreadExecutor();
        try(var lock=db.getConnection();var observer=db.getConnection();var probe=observer.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT id,event_id,published,%'")) {
            probe.setQueryTimeout(1);try(var warm=probe.executeQuery()){warm.next();}
            lock.setAutoCommit(false);lock.createStatement().execute("LOCK TABLE outbox_event IN ACCESS EXCLUSIVE MODE");
            var first=workers.submit(()->h.read(q));
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(()->h.read(q)).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.RESOURCE_EXHAUSTED));
            proceed.countDown();
            org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(5)).atMost(Duration.ofSeconds(1)).until(()->{
                try(var r=probe.executeQuery()){r.next();return r.getInt(1)>0;}
            });
            assertThatThrownBy(()->first.get(2,TimeUnit.SECONDS)).hasCauseInstanceOf(GatewayException.class);lock.rollback();
        }finally{proceed.countDown();workers.shutdownNow();}
        assertThat(h.read(q).items()).isEmpty();
        var writing=reader(db,(c,request,key)->{c.createStatement().executeUpdate("DELETE FROM outbox_event");return List.of();});
        assertThatThrownBy(()->writing.read(q)).isInstanceOf(GatewayException.class);
    }
    @Test void statementCancellationAndOwnerRateAreFinite() throws Exception {
        var slow=reader(db,(c,q,k)->{c.createStatement().execute("SELECT pg_sleep(2)");return List.of();});
        long started=System.nanoTime();assertThatThrownBy(()->slow.read(query(Kind.OUTBOX_EVENT,"owner",16))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.DEADLINE_EXCEEDED));
        assertThat(Duration.ofNanos(System.nanoTime()-started)).isLessThan(Duration.ofSeconds(1));
        var rate=new BoundedOwnerSearch(db,"test","owner","one",Map.of(Kind.OUTBOX_EVENT,Set.of("owner")),Map.of(Kind.OUTBOX_EVENT,MessagingSearchSource.OUTBOX_STATES),MessagingSearchSource::read);
        var q=new Query("test","owner",Kind.OUTBOX_EVENT,"owner",Instant.now().minusSeconds(20),Instant.now(),"ANY",16,"");
        rate.read(q);assertThatThrownBy(()->rate.read(q)).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.RESOURCE_EXHAUSTED));
    }
    @Test void invalidRangeSizeAndUnregisteredStateFailBeforeSql() {
        assertThatThrownBy(()->query(Kind.OUTBOX_EVENT,"owner",17)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->cursor(query(Kind.OUTBOX_EVENT,"owner",16),"x".repeat(257))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->new Query("test","owner",Kind.OUTBOX_EVENT,"owner",BASE.minusSeconds(3601),BASE,"ANY",1,"")).isInstanceOf(IllegalArgumentException.class);
        var h=reader();assertThatThrownBy(()->h.read(new Query("test","owner",Kind.OUTBOX_EVENT,"owner",BASE.minusSeconds(1),BASE,"CANARY_SECRET",1,""))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.INVALID_ARGUMENT));
    }
    @Test void clockStepDuringReadIsExplicitlyUncertain() {
        var h=reader(db,(c,q,k)->{clock.now=BASE.minusSeconds(2);return List.of();});
        var page=h.read(query(Kind.OUTBOX_EVENT,"owner",16));
        assertThat(page.coverage()).contains(Gap.CLOCK_UNCERTAIN);assertThat(page.scanCompletedAt()).isBefore(page.scanStartedAt());
    }
    @Test void representativeNativePlansAndReadDistributionsAreRecorded() throws Exception {
        try(var c=db.getConnection();var s=c.createStatement()) {
            s.execute("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload,created_at) SELECT md5(n::text)::uuid,'TEST',1,md5(n::text)::uuid,md5(n::text)::uuid,'TEST','owner','owner',now(),'{}',TIMESTAMPTZ '"+BASE.minusSeconds(10)+"' FROM generate_series(1,10000) n");
            s.execute("ANALYZE outbox_event");
            var lines=new ArrayList<String>();
            try(var r=s.executeQuery("EXPLAIN (ANALYZE, BUFFERS) SELECT id,event_id,published,dead_letter,next_retry_at,claimed_until,created_at FROM outbox_event WHERE id > 0 ORDER BY id LIMIT 16")) {
                while(r.next()){String line=r.getString(1);if(line.contains("Limit")||line.contains("Index Scan")||line.contains("Buffers:")||line.contains("Planning Time:")||line.contains("Execution Time:"))lines.add(line);}
            }
            assertThat(String.join("\n",lines)).contains("outbox_event_pkey","rows=16");
            Files.createDirectories(Path.of("target"));Files.writeString(Path.of("target/ops03c-plans.txt"),String.join("\n",lines));
        }
        StringBuilder csv=new StringBuilder("scenario,samples,p50Micros,p95Micros,p99Micros,currentThreadCpuNanos,currentThreadAllocatedBytes\n");
        var h=reader();var q=query(Kind.OUTBOX_EVENT,"owner",16);
        measure(csv,"nonempty_10000",h,q);
        var empty=new Query(q.environment(),q.owner(),q.kind(),q.scope(),BASE.minusSeconds(1100),BASE.minusSeconds(1000),"ANY",16,"");measure(csv,"empty_time_filtered_candidates_10000",h,empty);
        try(var c=db.getConnection();var s=c.createStatement()){s.execute("TRUNCATE outbox_event");}
        measure(csv,"empty_table",h,q);Files.writeString(Path.of("target/ops03c-cost.csv"),csv);
    }
    void measure(StringBuilder csv,String name,BoundedOwnerSearch h,Query q) {
        for(int n=0;n<20;n++)h.read(q);
        var bean=(com.sun.management.ThreadMXBean)java.lang.management.ManagementFactory.getThreadMXBean();long tid=Thread.currentThread().threadId();long cpu=bean.getCurrentThreadCpuTime(),bytes=bean.getThreadAllocatedBytes(tid);long[] times=new long[100];
        for(int n=0;n<times.length;n++){long t=System.nanoTime();var p=h.read(q);assertThat(p.candidatesExamined()).isLessThanOrEqualTo(16);if(name.startsWith("nonempty"))assertThat(p.items()).hasSize(16);else assertThat(p.items()).isEmpty();times[n]=System.nanoTime()-t;}
        cpu=bean.getCurrentThreadCpuTime()-cpu;bytes=bean.getThreadAllocatedBytes(tid)-bytes;Arrays.sort(times);
        assertThat(times[98]).isLessThan(250_000_000L);
        csv.append(name).append(",100,").append(times[49]/1000).append(',').append(times[94]/1000).append(',').append(times[98]/1000).append(',').append(cpu).append(',').append(bytes).append('\n');
    }
}

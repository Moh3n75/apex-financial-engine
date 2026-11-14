package com.apex.operations.owner;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.OwnerRecentSearch.*;
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

class OwnerRecentSearchTest {
    static EmbeddedPostgres pg;static DataSource db;static final Instant BASE=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    static class Time extends Clock {Instant now=BASE;public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId z){return this;}public Instant instant(){return now;}}
    final Time clock=new Time();final AtomicLong nanos=new AtomicLong();
    @BeforeAll static void start()throws Exception {
        pg=EmbeddedPostgres.builder().start();db=pg.getPostgresDatabase();
        new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),new ClassPathResource("db/platform/V1002__platform_processed_event.sql")).execute(db);
        try(var c=db.getConnection();var s=c.createStatement()) {
            s.execute("CREATE INDEX CONCURRENTLY ops03d_outbox_event_recent ON public.outbox_event(created_at DESC,id DESC)");
            s.execute("CREATE INDEX CONCURRENTLY ops03d_processed_event_recent ON public.processed_event(consumer_group,processed_at DESC,event_id DESC)");
        }
    }
    @AfterAll static void stop()throws Exception{if(pg!=null)pg.close();}
    @BeforeEach void clear()throws Exception{try(var c=db.getConnection();var s=c.createStatement()){s.execute("TRUNCATE outbox_event,processed_event RESTART IDENTITY");}}
    Query query(Kind k,String scope,int limit){return new Query("test","owner",k,scope,BASE.minusSeconds(300),BASE,"ANY",limit,"");}
    Query cursor(Query q,String next){return new Query(q.environment(),q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),q.state(),q.limit(),next);}
    BoundedRecentSearch reader(DataSource database,BoundedRecentSearch.Source source){return new BoundedRecentSearch(database,"test","owner","one",Map.of(Kind.OUTBOX_EVENT,Set.of("owner"),Kind.KNOWN_GROUP_INBOX,Set.of("known")),Map.of(Kind.OUTBOX_EVENT,MessagingRecentSource.OUTBOX_STATES,Kind.KNOWN_GROUP_INBOX,Set.of("HANDLER_TRANSACTION_COMMITTED")),source,clock,()->nanos.addAndGet(2_000_000_000L));}
    BoundedRecentSearch reader(){return reader(db,MessagingRecentSource::read);}
    UUID insert(Connection c,Instant time)throws Exception {
        UUID id=UUID.randomUUID();try(var s=c.prepareStatement("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload,created_at) VALUES (?,'TEST',1,?,?,'TEST','owner','owner',?,'{\"private\":\"CANARY_PAYLOAD\"}',?)")) {
            s.setObject(1,id);s.setObject(2,UUID.randomUUID());s.setObject(3,UUID.randomUUID());s.setObject(4,time.atOffset(ZoneOffset.UTC));s.setObject(5,time.atOffset(ZoneOffset.UTC));s.executeUpdate();
        }return id;
    }
    void retained(int count)throws Exception {
        try(var c=db.getConnection();var s=c.createStatement()){s.execute("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload,published,created_at) SELECT md5(n::text)::uuid,'TEST',1,md5(n::text)::uuid,md5(n::text)::uuid,'TEST','owner','owner',now(),'{}',true,TIMESTAMPTZ '"+BASE.minusSeconds(172800)+"' FROM generate_series(1,"+count+") n");s.execute("ANALYZE outbox_event");}
    }
    @Test void recentRangeFindsUnknownNewIdsPastHundredThousandOldRowsAndTiesPaginateExactly()throws Exception {
        retained(100000);var expected=new ArrayList<UUID>();try(var c=db.getConnection()){for(int n=0;n<40;n++)expected.add(insert(c,BASE.minusSeconds(1)));}
        var q=query(Kind.OUTBOX_EVENT,"owner",16);var h=reader();var actual=new ArrayList<UUID>();Page page;
        do{page=h.read(q);actual.addAll(page.items().stream().map(Item::ref).toList());if(!page.nextCursor().isEmpty())q=cursor(q,page.nextCursor());}while(!page.nextCursor().isEmpty());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected).doesNotHaveDuplicates();
        try(var c=db.getConnection();var s=c.createStatement();var rs=s.executeQuery("SELECT event_id FROM outbox_event WHERE created_at>=TIMESTAMPTZ '"+BASE.minusSeconds(300)+"' ORDER BY created_at DESC,id DESC")) {
            var sql=new ArrayList<UUID>();while(rs.next())sql.add(rs.getObject(1,UUID.class));assertThat(actual).containsExactlyElementsOf(sql);
        }
        assertThat(page.coverage()).contains(Gap.INDEXED_RECENT_RANGE,Gap.LATE_COMMIT_GAP,Gap.RETENTION_UNQUALIFIED);assertThat(page.toString()).doesNotContain("CANARY_");
    }
    @Test void completedDescWalkMissesLaterCommitAheadOfCursorAndBoundedRestartConverges()throws Exception {
        var h=reader();var q=query(Kind.OUTBOX_EVENT,"owner",1);
        try(var a=db.getConnection();var b=db.getConnection()) {
            a.setAutoCommit(false);UUID late=insert(a,BASE.minusSeconds(120));UUID visible=insert(b,BASE.minusSeconds(20));
            var first=h.read(q);assertThat(first.items()).extracting(Item::ref).containsExactly(visible);
            assertThat(h.read(cursor(q,first.nextCursor())).items()).isEmpty();a.commit();
            // A source timestamp predates B; the completed walk cannot know it will commit later.
            UUID newer=insert(b,BASE.minusSeconds(1));var restarted=h.read(query(Kind.OUTBOX_EVENT,"owner",16));
            assertThat(restarted.items()).extracting(Item::ref).containsExactly(newer,visible,late);
            a.setAutoCommit(false);UUID rollback=insert(a,BASE.minusSeconds(2));a.rollback();
            assertThat(h.read(query(Kind.OUTBOX_EVENT,"owner",16)).items()).extracting(Item::ref).doesNotContain(rollback);
            UUID expired=insert(b,BASE.minusSeconds(3601));assertThat(h.read(query(Kind.OUTBOX_EVENT,"owner",16)).items()).extracting(Item::ref).doesNotContain(expired);
        }
    }
    @Test void sourceBoundsAreInclusiveExclusiveAndPostFilterCannotPretendComplete()throws Exception {
        var q=query(Kind.OUTBOX_EVENT,"owner",1);UUID included;try(var c=db.getConnection()){included=insert(c,q.fromInclusive());insert(c,q.toExclusive());}
        var h=reader();assertThat(h.read(q).items()).extracting(Item::ref).containsExactly(included);
        var noMatch=new Query(q.environment(),q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),"PUBLISHER_MARK_COMMITTED",1,"");
        var page=h.read(noMatch);assertThat(page.items()).isEmpty();assertThat(page.candidatesExamined()).isEqualTo(1);assertThat(page.coverage()).contains(Gap.STATE_POST_FILTERED,Gap.CANDIDATE_PAGE_LIMIT);
    }
    @Test void denseRecentWindowStopsAtEightPagesAndNeverCallsItComplete()throws Exception {
        try(var c=db.getConnection()){for(int n=0;n<140;n++)insert(c,BASE.minusSeconds(1));}
        var h=reader();var q=query(Kind.OUTBOX_EVENT,"owner",16);int count=0;Page page;
        do{page=h.read(q);count+=page.items().size();if(!page.nextCursor().isEmpty())q=cursor(q,page.nextCursor());}while(!page.nextCursor().isEmpty());
        assertThat(count).isEqualTo(128);assertThat(page.coverage()).contains(Gap.WALK_LIMIT);
    }
    @Test void cursorTiesBindScopeWindowStateLimitExpiryAndEpoch() {
        var q=query(Kind.OUTBOX_EVENT,"owner",16);var h=new RecentCursor(clock);String token=h.next(q,h.read(q),"11",BASE.minusSeconds(1));assertThat(token.length()).isLessThanOrEqualTo(256);
        assertThat(h.read(cursor(q,token)).time()).isEqualTo(BASE.minusSeconds(1));
        assertThatThrownBy(()->h.read(cursor(q,token.substring(0,12)+"!"+token.substring(13)))).isInstanceOf(IllegalArgumentException.class);
        for(var changed:List.of(new Query("test","owner",Kind.OUTBOX_EVENT,"owner",q.fromInclusive(),q.toExclusive(),"ANY",1,""),new Query("test","owner",Kind.KNOWN_GROUP_INBOX,"known",q.fromInclusive(),q.toExclusive(),"ANY",16,""),new Query("test","other",Kind.OUTBOX_EVENT,"owner",q.fromInclusive(),q.toExclusive(),"ANY",16,""),new Query("test","owner",Kind.OUTBOX_EVENT,"owner",q.fromInclusive().plusSeconds(1),q.toExclusive(),"ANY",16,"")))assertThatThrownBy(()->h.read(cursor(changed,token))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->new RecentCursor(clock).read(cursor(q,token))).isInstanceOf(IllegalArgumentException.class);clock.now=BASE.plusSeconds(31);assertThatThrownBy(()->h.read(cursor(q,token))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void missingOrWrongIndexFailsClosedInsteadOfScanning()throws Exception {
        try(var c=db.getConnection();var s=c.createStatement()) {
            s.execute("DROP INDEX ops03d_outbox_event_recent");
            try{assertThatThrownBy(()->reader().read(query(Kind.OUTBOX_EVENT,"owner",16))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.FAILED_PRECONDITION));
                s.execute("CREATE INDEX ops03d_outbox_event_recent ON outbox_event(id,created_at)");
                assertThatThrownBy(()->reader().read(query(Kind.OUTBOX_EVENT,"owner",16))).isInstanceOf(GatewayException.class);
            }finally{s.execute("DROP INDEX IF EXISTS ops03d_outbox_event_recent");s.execute("CREATE INDEX ops03d_outbox_event_recent ON outbox_event(created_at DESC,id DESC)");}
        }
    }
    @Test void knownGroupAndEffectiveReadRoleRemainRestrictedAfterRevocation()throws Exception {
        try(var c=db.getConnection();var s=c.createStatement()) {
            s.execute("INSERT INTO processed_event SELECT 'known',md5(('old'||n)::text)::uuid,'TEST',1,'test',0,n,TIMESTAMPTZ '"+BASE.minusSeconds(172800)+"' FROM generate_series(1,100000) n");
            s.execute("INSERT INTO processed_event SELECT 'known',md5(n::text)::uuid,'TEST',1,'test',0,n,TIMESTAMPTZ '"+BASE.minusSeconds(1)+"' FROM generate_series(1,40) n");
            s.execute("INSERT INTO processed_event VALUES ('known','00000000-0000-0000-0000-000000000000','TEST',1,'test',0,0,TIMESTAMPTZ '"+BASE.minusSeconds(1)+"')");
            s.execute("INSERT INTO processed_event SELECT 'other',md5(n::text)::uuid,'TEST',1,'test',0,n,TIMESTAMPTZ '"+BASE.minusSeconds(1)+"' FROM generate_series(1,40) n");
            s.execute("CREATE ROLE ops03d_reader LOGIN");s.execute("GRANT USAGE ON SCHEMA public TO ops03d_reader");s.execute("GRANT SELECT(consumer_group,event_id,processed_at) ON processed_event TO ops03d_reader");
        }
        String inboxPlan=plan("SELECT event_id,processed_at FROM processed_event WHERE consumer_group='known' AND processed_at>=TIMESTAMPTZ '"+BASE.minusSeconds(300)+"' AND processed_at<TIMESTAMPTZ '"+BASE+"' ORDER BY processed_at DESC,event_id DESC LIMIT 16");
        assertThat(inboxPlan).contains("ops03d_processed_event_recent","Index Cond:","rows=16").doesNotContain("Seq Scan");
        Files.writeString(Path.of("target/ops03d-inbox-plan.txt"),inboxPlan);
        var restricted=new DriverManagerDataSource(pg.getJdbcUrl("ops03d_reader","postgres"),"ops03d_reader","");
        try(var c=restricted.getConnection();var s=c.createStatement();var rs=s.executeQuery("SELECT current_user")){rs.next();assertThat(rs.getString(1)).isEqualTo("ops03d_reader");}
        var h=reader(restricted,MessagingRecentSource::read);var q=query(Kind.KNOWN_GROUP_INBOX,"known",16);var first=h.read(q);assertThat(first.items()).hasSize(16);
        var seen=new ArrayList<UUID>(first.items().stream().map(Item::ref).toList());var page=first;
        while(!page.nextCursor().isEmpty()){page=h.read(cursor(q,page.nextCursor()));seen.addAll(page.items().stream().map(Item::ref).toList());}
        var sorted=new ArrayList<UUID>();
        try(var c=db.getConnection();var s=c.createStatement();var rs=s.executeQuery("SELECT event_id FROM processed_event WHERE consumer_group='known' AND processed_at>=TIMESTAMPTZ '"+BASE.minusSeconds(300)+"' ORDER BY processed_at DESC,event_id DESC")){while(rs.next())sorted.add(rs.getObject(1,UUID.class));}
        assertThat(seen).hasSize(41).containsExactlyElementsOf(sorted).contains(new UUID(0,0)).doesNotHaveDuplicates();
        assertThatThrownBy(()->h.read(query(Kind.KNOWN_GROUP_INBOX,"other",16))).isInstanceOf(GatewayException.class);
        try(var c=db.getConnection();var s=c.createStatement()){s.execute("REVOKE SELECT ON processed_event FROM ops03d_reader");}
        assertThatThrownBy(()->h.read(q)).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
    }
    @Test void locksStatementDeadlineRateAndReadOnlyTransactionBoundWork()throws Exception {
        var q=query(Kind.OUTBOX_EVENT,"owner",16);
        try(var c=db.getConnection();var s=c.createStatement()) {
            c.setAutoCommit(false);s.execute("LOCK outbox_event IN ACCESS EXCLUSIVE MODE");long t=System.nanoTime();
            assertThatThrownBy(()->reader().read(q)).isInstanceOf(GatewayException.class);assertThat(System.nanoTime()-t).isLessThan(1_000_000_000L);c.rollback();
        }
        assertThat(reader().read(q).items()).isEmpty();
        var slow=reader(db,(c,x,k)->{c.createStatement().execute("SELECT pg_sleep(2)");return List.of();});assertThatThrownBy(()->slow.read(q)).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.DEADLINE_EXCEEDED));
        var write=reader(db,(c,x,k)->{c.createStatement().execute("DELETE FROM outbox_event");return List.of();});assertThatThrownBy(()->write.read(q)).isInstanceOf(GatewayException.class);
        var rate=new BoundedRecentSearch(db,"test","owner","one",Map.of(Kind.OUTBOX_EVENT,Set.of("owner")),Map.of(Kind.OUTBOX_EVENT,MessagingRecentSource.OUTBOX_STATES),MessagingRecentSource::read);
        rate.read(q);assertThatThrownBy(()->rate.read(q)).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.RESOURCE_EXHAUSTED));
    }
    @Test void concurrentAdmissionHasNoQueueAndEpochInvalidationRequiresRestart()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var executor=Executors.newSingleThreadExecutor();
        var h=reader(db,(c,q,p)->{entered.countDown();try{if(!release.await(1,TimeUnit.SECONDS))throw new SQLException("TEST_GATE");}catch(InterruptedException e){Thread.currentThread().interrupt();throw new SQLException("TEST_INTERRUPTED");}return List.of();});
        var q=query(Kind.OUTBOX_EVENT,"owner",16);
        try {
            var first=executor.submit(()->h.read(q));assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(()->h.read(q)).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.RESOURCE_EXHAUSTED));release.countDown();assertThat(first.get(1,TimeUnit.SECONDS).items()).isEmpty();
        }finally{release.countDown();executor.shutdownNow();assertThat(executor.awaitTermination(2,TimeUnit.SECONDS)).isTrue();}
        try(var c=db.getConnection()){insert(c,BASE.minusSeconds(1));}
        var single=query(Kind.OUTBOX_EVENT,"owner",1);String token=reader().read(single).nextCursor();
        assertThatThrownBy(()->reader().read(cursor(single,token))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.FAILED_PRECONDITION));
    }
    @Test void ownerClockStepIsExplicitlyUncertainInsteadOfFreshProgress() {
        var h=reader(db,(c,q,p)->{clock.now=BASE.minusSeconds(2);return List.of();});
        var page=h.read(query(Kind.OUTBOX_EVENT,"owner",16));
        assertThat(page.coverage()).contains(Gap.CLOCK_UNCERTAIN,Gap.LATE_COMMIT_GAP);
        assertThat(page.scanCompletedAt()).isBefore(page.scanStartedAt());
    }
    @Test void disposableDatabaseRestartRetainsOwnerFactsAndOutageIsUnavailable()throws Exception {
        Path data=Files.createTempDirectory(Path.of("target"),"ops03d-owned-db-");int port;
        var owned=EmbeddedPostgres.builder().setCleanDataDirectory(false).setDataDirectory(data).start();
        try {
            port=owned.getPort();var source=owned.getPostgresDatabase();
            new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),new ClassPathResource("db/platform/V1002__platform_processed_event.sql")).execute(source);
            UUID id;try(var c=source.getConnection();var s=c.createStatement()){s.execute("CREATE INDEX ops03d_outbox_event_recent ON outbox_event(created_at DESC,id DESC)");id=insert(c,BASE.minusSeconds(1));}
            var h=reader(source,MessagingRecentSource::read);var q=query(Kind.OUTBOX_EVENT,"owner",16);assertThat(h.read(q).items()).extracting(Item::ref).containsExactly(id);
            owned.close();assertThatThrownBy(()->h.read(q)).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.UNAVAILABLE));
            owned=EmbeddedPostgres.builder().setCleanDataDirectory(false).setDataDirectory(data).setPort(port).start();
            assertThat(h.read(q).items()).extracting(Item::ref).containsExactly(id);
        }finally{owned.close();}
        // Retain this tiny owned data directory as failure evidence; never recursively delete unknown paths.
    }
    @Test void additiveIndexWriteCostIsMeasuredWithoutClaimingApprovedProductionBudget()throws Exception {
        var csv=new StringBuilder("indexed,repetition,rows,insertCommitMicros,indexBytes\n");
        try(var c=db.getConnection();var s=c.createStatement()) {
            for(boolean indexed:List.of(false,true)) {
                if(!indexed)s.execute("DROP INDEX ops03d_outbox_event_recent");else s.execute("CREATE INDEX CONCURRENTLY ops03d_outbox_event_recent ON outbox_event(created_at DESC,id DESC)");
                for(int repetition=0;repetition<3;repetition++) {
                    s.execute("TRUNCATE outbox_event RESTART IDENTITY");long begin=System.nanoTime();
                    s.execute("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload,created_at) SELECT md5(n::text)::uuid,'TEST',1,md5(n::text)::uuid,md5(n::text)::uuid,'TEST','owner','owner',now(),'{}',now() FROM generate_series(1,5000) n");
                    long elapsed=System.nanoTime()-begin;long bytes=0;
                    if(indexed)try(var rs=s.executeQuery("SELECT pg_relation_size('ops03d_outbox_event_recent')")){rs.next();bytes=rs.getLong(1);}
                    csv.append(indexed).append(',').append(repetition).append(",5000,").append(elapsed/1000).append(',').append(bytes).append('\n');
                }
            }
        }finally{
            try(var c=db.getConnection();var s=c.createStatement()){s.execute("CREATE INDEX IF NOT EXISTS ops03d_outbox_event_recent ON outbox_event(created_at DESC,id DESC)");}
        }
        Files.writeString(Path.of("target/ops03d-index-write-cost.csv"),csv);
    }
    @Test void indexedAndNonindexedPlansCostsAndBloatAreRecordedBeforeAnyProjectionGate()throws Exception {
        Files.createDirectories(Path.of("target"));var plans=new StringBuilder();var csv=new StringBuilder("retained,scenario,samples,p50Micros,p95Micros,p99Micros,threadCpuNanos,threadAllocatedBytes\n");
        for(int count:List.of(10000,100000,250000)) {
            try(var c=db.getConnection();var s=c.createStatement()){s.execute("TRUNCATE outbox_event RESTART IDENTITY");}retained(count);
            try(var c=db.getConnection()){for(int n=0;n<40;n++)insert(c,BASE.minusSeconds(1));}
            String sql="SELECT id,event_id,published,dead_letter,next_retry_at,claimed_until,created_at FROM outbox_event WHERE created_at>=TIMESTAMPTZ '"+BASE.minusSeconds(300)+"' AND created_at<TIMESTAMPTZ '"+BASE+"' ORDER BY created_at DESC,id DESC LIMIT 16";
            String indexed=plan(sql);plans.append(count+" indexed\n"+indexed+"\n");assertThat(indexed).contains("ops03d_outbox_event_recent","Index Cond:","rows=16").doesNotContain("Seq Scan");
            measure(csv,count,"matching",reader(),query(Kind.OUTBOX_EVENT,"owner",16));
            var q=query(Kind.OUTBOX_EVENT,"owner",16);measure(csv,count,"empty",reader(),new Query(q.environment(),q.owner(),q.kind(),q.scope(),BASE.minusSeconds(200),BASE.minusSeconds(100),"ANY",16,""));
            try(var c=db.getConnection();var s=c.createStatement()){s.execute("DROP INDEX ops03d_outbox_event_recent");}
            try{plans.append(count+" nonindexed control NOT served by production reader\n"+plan(sql)+"\n");}
            finally{try(var c=db.getConnection();var s=c.createStatement()){s.execute("CREATE INDEX CONCURRENTLY ops03d_outbox_event_recent ON outbox_event(created_at DESC,id DESC)");}}
            if(count==250000){try(var c=db.getConnection();var s=c.createStatement()){s.execute("DELETE FROM outbox_event WHERE id<=10000");}plans.append("deleted old rows no vacuum\n"+plan(sql)+"\n");}
        }
        Files.writeString(Path.of("target/ops03d-native-plans.txt"),plans);Files.writeString(Path.of("target/ops03d-native-cost.csv"),csv);
    }
    String plan(String sql)throws Exception{var out=new StringBuilder();try(var c=db.getConnection();var s=c.createStatement();var rs=s.executeQuery("EXPLAIN (ANALYZE,BUFFERS) "+sql)){while(rs.next())out.append(rs.getString(1)).append('\n');}return out.toString();}
    void measure(StringBuilder csv,int count,String name,BoundedRecentSearch h,Query q) {
        for(int n=0;n<20;n++)h.read(q);var bean=(com.sun.management.ThreadMXBean)java.lang.management.ManagementFactory.getThreadMXBean();long tid=Thread.currentThread().threadId(),cpu=bean.getCurrentThreadCpuTime(),bytes=bean.getThreadAllocatedBytes(tid);long[] times=new long[100];
        for(int n=0;n<100;n++){long t=System.nanoTime();var page=h.read(q);if(name.equals("matching"))assertThat(page.items()).hasSize(16);else assertThat(page.items()).isEmpty();times[n]=System.nanoTime()-t;}
        Arrays.sort(times);csv.append(count).append(',').append(name).append(",100,").append(times[49]/1000).append(',').append(times[94]/1000).append(',').append(times[98]/1000).append(',').append(bean.getCurrentThreadCpuTime()-cpu).append(',').append(bean.getThreadAllocatedBytes(tid)-bytes).append('\n');
        // Preserve distributions even if a preregistered exploratory cost gate is exceeded.
        try{Files.writeString(Path.of("target/ops03d-native-cost.csv"),csv);}catch(Exception e){throw new RuntimeException(e);}
        assertThat(times[94]).isLessThan(150_000_000L);assertThat(times[98]).isLessThan(250_000_000L);
    }
}

package com.apex.payment.operations;
import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.OwnerRecentSearch.*;
import com.apex.platform.grpc.api.GatewayException;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.time.*;import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;
class PaymentRecentSearchTest {
    static EmbeddedPostgres pg;static PaymentEvidenceDataSource.Pool pool;
    static MockEnvironment env(){return new MockEnvironment().withProperty("apex.grpc.security.enabled","true").withProperty("apex.ops03d.indexes-reviewed","true").withProperty("apex.ops03.environment","test").withProperty("apex.ops03.instance","one").withProperty("apex.ops03d.scopes","WORKFLOW:authorization-simulation");}
    @BeforeAll static void start()throws Exception {
        pg=EmbeddedPostgres.builder().start();org.flywaydb.core.Flyway.configure().dataSource(pg.getPostgresDatabase()).locations("classpath:db/platform","classpath:db/migration").load().migrate();
        var e=env().withProperty("apex.ops03.jdbc-url",pg.getJdbcUrl("postgres","postgres")).withProperty("apex.ops03.jdbc-user","postgres").withProperty("apex.ops03.jdbc-password","");
        pool=new PaymentEvidenceDataSource().evidencePool(e);
        String ddl=new String(new org.springframework.core.io.ClassPathResource("db/ops03d/recent-indexes.sql").getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8);
        ddl=ddl.replaceAll("(?m)--.*$","");
        try(var c=pg.getPostgresDatabase().getConnection();var statement=c.createStatement()){for(String command:ddl.split(";"))if(!command.isBlank())statement.execute(command);}

    }
    @AfterAll static void close()throws Exception {if(pool!=null)pool.close();if(pg!=null)pg.close();}

    @BeforeEach void clearOwnerRows()throws Exception {try(var c=pg.getPostgresDatabase().getConnection();var s=c.createStatement()){s.execute("TRUNCATE payment_authorizations CASCADE");}}
    @Test void recentIndexFindsNewRowsAmongLargeRetainedCatalogAndEmptyRange()throws Exception {
        var plan=new ArrayList<String>();
        try(var c=pg.getPostgresDatabase().getConnection();var s=c.createStatement()) {
            s.execute("INSERT INTO payment_authorizations(order_id,status,created_at) SELECT md5('plan'||n)::uuid,'AUTHORIZED',now()-interval '2 hours' FROM generate_series(1,100000) n");s.execute("INSERT INTO payment_authorizations(order_id,status,created_at) SELECT md5('recent'||n)::uuid,'AUTHORIZED',now()-interval '1 second' FROM generate_series(1,20) n");s.execute("ANALYZE payment_authorizations");
            try(var r=s.executeQuery("EXPLAIN (ANALYZE, BUFFERS) SELECT order_id,status,created_at FROM payment_authorizations WHERE created_at>=now()-interval '5 minutes' AND created_at<now() ORDER BY created_at DESC,order_id DESC LIMIT 16")) {
                while(r.next()){String line=r.getString(1);if(line.contains("Limit")||line.contains("Index Scan")||line.contains("Index Cond:")||line.contains("Buffers:")||line.contains("Planning Time:")||line.contains("Execution Time:"))plan.add(line);}
            }
        }
        assertThat(String.join("\n",plan)).contains("ops03d_payment_authorizations_recent","Index Cond:","rows=16");
        var handler=new PaymentRecentSearchHandler(pool,env());var selected=query();var page=handler.search(selected);assertThat(page.candidatesExamined()).isEqualTo(16);assertThat(page.items()).hasSize(16);assertThat(page.coverage()).contains(Gap.CANDIDATE_PAGE_LIMIT);
        var seen=new ArrayList<UUID>(page.items().stream().map(Item::ref).toList());
        assertThat(page.nextCursor()).isNotEmpty();
        var continued=new Query(selected.environment(),selected.owner(),selected.kind(),selected.scope(),selected.fromInclusive(),selected.toExclusive(),selected.state(),selected.limit(),page.nextCursor());
        var observed=new java.util.concurrent.atomic.AtomicReference<Page>();
        org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(2)).until(()->{
            try{observed.set(handler.search(continued));return true;}catch(GatewayException expected){if(expected.code()==GatewayException.Code.RESOURCE_EXHAUSTED)return false;throw expected;}
        });
        assertThat(observed.get().items()).hasSize(4);assertThat(observed.get().nextCursor()).isEmpty();seen.addAll(observed.get().items().stream().map(Item::ref).toList());
        var independentlySorted=new ArrayList<UUID>();
        try(var connection=pg.getPostgresDatabase().getConnection();var statement=connection.prepareStatement("SELECT order_id FROM payment_authorizations WHERE created_at>=? AND created_at<? ORDER BY created_at DESC,order_id DESC")){
            statement.setObject(1,selected.fromInclusive().atOffset(ZoneOffset.UTC));statement.setObject(2,selected.toExclusive().atOffset(ZoneOffset.UTC));try(var rows=statement.executeQuery()){while(rows.next())independentlySorted.add(rows.getObject(1,UUID.class));}
        }
        assertThat(seen).containsExactlyElementsOf(independentlySorted).doesNotHaveDuplicates();
        var base=query();var empty=new Query(base.environment(),base.owner(),base.kind(),base.scope(),base.fromInclusive(),base.fromInclusive().plusSeconds(1),"ANY",16,"");
        assertThat(new PaymentRecentSearchHandler(pool,env()).search(empty).items()).isEmpty();
        java.nio.file.Files.writeString(java.nio.file.Path.of("target/ops03d-payment-cell-plan.txt"),String.join("\n",plan));
    }

    @Test void exhaustedOperationalPoolIsFiniteAndSameSearchHandlerRecovers()throws Exception {
        var h=new PaymentRecentSearchHandler(pool,env());var q=query();long started=System.nanoTime();
        try(var held=pool.dataSource().getConnection()) {assertThatThrownBy(()->h.search(q)).isInstanceOf(GatewayException.class);}
        assertThat(Duration.ofNanos(System.nanoTime()-started)).isLessThan(Duration.ofSeconds(1));
        org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(2)).untilAsserted(()->{
            try{assertThat(h.search(q).items()).isEmpty();}catch(GatewayException e){assertThat(e.code()).isEqualTo(GatewayException.Code.RESOURCE_EXHAUSTED);throw new AssertionError("expected admission cooldown",e);}
        });
    }
    Query query(){return new Query("test","payment-cell",Kind.WORKFLOW,"authorization-simulation",Instant.now().minusSeconds(300),Instant.now(),"ANY",16,"");}
    @Test void actualOwnerStatesAndPrimaryIndexExposeOnlyCurrentMetadata()throws Exception {
        try(var c=pg.getPostgresDatabase().getConnection()){for(String state:List.of("PENDING","AUTHORIZED","DECLINED")){var id=UUID.randomUUID();try(var s=c.prepareStatement("INSERT INTO payment_authorizations(order_id,status,created_at) VALUES (?, ?, now()-interval '1 second')")){s.setObject(1,id);s.setString(2,state);s.executeUpdate();}}}
        var page=new PaymentRecentSearchHandler(pool,env()).search(query());assertThat(page.items()).extracting(Item::state).contains("PENDING","AUTHORIZED","DECLINED");
        assertThat(page.coverage()).contains(Gap.PARTIAL_CURRENT_STATE,Gap.RETENTION_UNQUALIFIED);assertThat(page.toString()).doesNotContain("CANARY_","payload","description");
    }
    @Test void insecureAndUnregisteredScopeConfigurationsFailClosed(){
        assertThatThrownBy(()->new PaymentRecentSearchHandler(pool,env().withProperty("apex.ops03d.indexes-reviewed","false"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(()->new PaymentRecentSearchHandler(pool,env().withProperty("apex.grpc.security.enabled","false"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(()->new PaymentRecentSearchHandler(pool,env().withProperty("apex.grpc.max-payload-bytes","65537"))).isInstanceOf(IllegalStateException.class).hasMessage("OPS_SEARCH_WIRE_POLICY");
        assertThatThrownBy(()->new PaymentRecentSearchHandler(pool,env().withProperty("apex.ops03d.scopes","WORKFLOW:other"))).isInstanceOf(IllegalArgumentException.class);
        var h=new PaymentRecentSearchHandler(pool,env());var q=query();
        assertThatThrownBy(()->h.search(new Query("other",q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),"ANY",16,""))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
    }

}

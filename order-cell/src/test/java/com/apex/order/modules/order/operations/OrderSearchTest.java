package com.apex.order.modules.order.operations;
import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.OwnerSearch.*;
import com.apex.platform.grpc.api.GatewayException;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.time.*;import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;
class OrderSearchTest {
    static EmbeddedPostgres pg;static OrderEvidenceDataSource.Pool pool;
    static MockEnvironment env(){return new MockEnvironment().withProperty("apex.grpc.security.enabled","true").withProperty("apex.ops03.environment","test").withProperty("apex.ops03.instance","one").withProperty("apex.ops03c.scopes","WORKFLOW:order");}
    @BeforeAll static void start()throws Exception {
        pg=EmbeddedPostgres.builder().start();org.flywaydb.core.Flyway.configure().dataSource(pg.getPostgresDatabase()).locations("classpath:db/platform","classpath:db/migration").load().migrate();
        var e=env().withProperty("apex.ops03.jdbc-url",pg.getJdbcUrl("postgres","postgres")).withProperty("apex.ops03.jdbc-user","postgres").withProperty("apex.ops03.jdbc-password","");
        pool=new OrderEvidenceDataSource().evidencePool(e);
    }
    @AfterAll static void close()throws Exception {if(pool!=null)pool.close();if(pg!=null)pg.close();}

    @BeforeEach void clearOwnerRows()throws Exception {try(var c=pg.getPostgresDatabase().getConnection();var s=c.createStatement()){s.execute("TRUNCATE orders CASCADE");}}
    @Test void primaryIndexCandidateBudgetHoldsWithLargeAndEmptyTimeRanges()throws Exception {
        var plan=new ArrayList<String>();
        try(var c=pg.getPostgresDatabase().getConnection();var s=c.createStatement()) {
            s.execute("INSERT INTO orders(id,external_reference,description,status,created_at) SELECT md5('plan'||n)::uuid,'plan-'||n,'CANARY_PAYLOAD','CONFIRMED',now()-interval '2 hours' FROM generate_series(1,10000) n");s.execute("ANALYZE orders");
            try(var r=s.executeQuery("EXPLAIN (ANALYZE, BUFFERS) SELECT id,status,created_at FROM orders ORDER BY id LIMIT 16")) {
                while(r.next()){String line=r.getString(1);if(line.contains("Limit")||line.contains("Index Scan")||line.contains("Buffers:")||line.contains("Planning Time:")||line.contains("Execution Time:"))plan.add(line);}
            }
        }
        assertThat(String.join("\n",plan)).contains("orders_pkey","rows=16");
        var page=new OrderSearchHandler(pool,env()).search(query());assertThat(page.candidatesExamined()).isEqualTo(16);assertThat(page.items()).isEmpty();assertThat(page.coverage()).contains(Gap.CANDIDATE_PAGE_LIMIT);
        java.nio.file.Files.writeString(java.nio.file.Path.of("target/ops03c-order-cell-plan.txt"),String.join("\n",plan));
    }

    @Test void exhaustedOperationalPoolIsFiniteAndSameSearchHandlerRecovers()throws Exception {
        var h=new OrderSearchHandler(pool,env());var q=query();long started=System.nanoTime();
        try(var held=pool.dataSource().getConnection()) {assertThatThrownBy(()->h.search(q)).isInstanceOf(GatewayException.class);}
        assertThat(Duration.ofNanos(System.nanoTime()-started)).isLessThan(Duration.ofSeconds(1));
        org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(2)).untilAsserted(()->{
            try{assertThat(h.search(q).items()).isEmpty();}catch(GatewayException e){assertThat(e.code()).isEqualTo(GatewayException.Code.RESOURCE_EXHAUSTED);throw new AssertionError("expected admission cooldown",e);}
        });
    }
    Query query(){return new Query("test","order-cell",Kind.WORKFLOW,"order",Instant.now().minusSeconds(300),Instant.now(),"ANY",16,"");}
    @Test void actualOwnerStatesAndPrimaryIndexExposeOnlyCurrentMetadata()throws Exception {
        try(var c=pg.getPostgresDatabase().getConnection()){for(String state:List.of("PENDING_INVENTORY","PENDING_PAYMENT","CANCELLING","CONFIRMED","CANCELLED","CREATED")){var id=UUID.randomUUID();try(var s=c.prepareStatement("INSERT INTO orders(id,external_reference,description,status,created_at) VALUES (?, ?, 'CANARY_PAYLOAD', ?, now()-interval '1 second')")){s.setObject(1,id);s.setString(2,id.toString());s.setString(3,state);s.executeUpdate();}}}
        var page=new OrderSearchHandler(pool,env()).search(query());assertThat(page.items()).extracting(Item::state).contains("PENDING_INVENTORY","PENDING_PAYMENT","CANCELLING","CONFIRMED","CANCELLED","CREATED");
        assertThat(page.coverage()).contains(Gap.PARTIAL_CURRENT_STATE,Gap.RETENTION_UNQUALIFIED);assertThat(page.toString()).doesNotContain("CANARY_","payload","description");
    }
    @Test void insecureAndUnregisteredScopeConfigurationsFailClosed(){
        assertThatThrownBy(()->new OrderSearchHandler(pool,env().withProperty("apex.grpc.security.enabled","false"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(()->new OrderSearchHandler(pool,env().withProperty("apex.grpc.max-payload-bytes","65537"))).isInstanceOf(IllegalStateException.class).hasMessage("OPS_SEARCH_WIRE_POLICY");
        assertThatThrownBy(()->new OrderSearchHandler(pool,env().withProperty("apex.ops03c.scopes","WORKFLOW:other"))).isInstanceOf(IllegalArgumentException.class);
        var h=new OrderSearchHandler(pool,env());var q=query();
        assertThatThrownBy(()->h.search(new Query("other",q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),"ANY",16,""))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
    }

    @Test void scopedReceiptsAreIndependentOfEventsAndNeverExportIdentityOrResponse() throws Exception {
        var id=UUID.randomUUID();
        try(var c=pg.getPostgresDatabase().getConnection();var s=c.prepareStatement("INSERT INTO order_request_receipt(idempotency_key,client_id,tenant_id,principal,contract_type,contract_version,request_id,fingerprint,response,created_at,completed_at) VALUES ('CANARY_KEY','fixture-client','CANARY_TENANT','CANARY_PRINCIPAL','PLACE_ORDER',1,?,repeat('x',64),decode('43414e415259','hex'),now(),now())")){s.setObject(1,id);s.executeUpdate();}
        var enabled=env().withProperty("apex.ops03c.receipt-client","fixture-client").withProperty("apex.ops03c.scopes","REQUEST_RECEIPT:order-request");
        var h=new OrderSearchHandler(pool,enabled);var q=new Query("test","order-cell",Kind.REQUEST_RECEIPT,"order-request",Instant.now().minusSeconds(30),Instant.now(),"ANY",16,"");
        var page=h.search(q);assertThat(page.items()).extracting(Item::ref).contains(id);assertThat(page.toString()).contains("RECEIPT_COMMITTED").doesNotContain("CANARY_","fixture-client","response=","principal=");
        assertThatThrownBy(()->new OrderSearchHandler(pool,env().withProperty("apex.ops03c.scopes","REQUEST_RECEIPT:order-request"))).isInstanceOf(IllegalArgumentException.class);
    }

}

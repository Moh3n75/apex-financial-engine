package com.apex.order.modules.order.operations;
import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.OwnerRecentSearch.*;
import com.apex.platform.grpc.api.GatewayException;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.time.*;import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;
class OrderRecentSearchTest {
    static EmbeddedPostgres pg;static OrderEvidenceDataSource.Pool pool;
    static MockEnvironment env(){return new MockEnvironment().withProperty("apex.grpc.security.enabled","true").withProperty("apex.ops03d.indexes-reviewed","true").withProperty("apex.ops03.environment","test").withProperty("apex.ops03.instance","one").withProperty("apex.ops03d.scopes","WORKFLOW:order");}
    @BeforeAll static void start()throws Exception {
        pg=EmbeddedPostgres.builder().start();org.flywaydb.core.Flyway.configure().dataSource(pg.getPostgresDatabase()).locations("classpath:db/platform","classpath:db/migration").load().migrate();
        var e=env().withProperty("apex.ops03.jdbc-url",pg.getJdbcUrl("postgres","postgres")).withProperty("apex.ops03.jdbc-user","postgres").withProperty("apex.ops03.jdbc-password","");
        pool=new OrderEvidenceDataSource().evidencePool(e);
        String ddl=new String(new org.springframework.core.io.ClassPathResource("db/ops03d/recent-indexes.sql").getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8);
        ddl=ddl.replaceAll("(?m)--.*$","");
        try(var c=pg.getPostgresDatabase().getConnection();var statement=c.createStatement()){for(String command:ddl.split(";"))if(!command.isBlank())statement.execute(command);}

    }
    @AfterAll static void close()throws Exception {if(pool!=null)pool.close();if(pg!=null)pg.close();}

    @BeforeEach void clearOwnerRows()throws Exception {try(var c=pg.getPostgresDatabase().getConnection();var s=c.createStatement()){s.execute("TRUNCATE orders CASCADE");}}
    @Test void recentIndexFindsNewRowsAmongLargeRetainedCatalogAndEmptyRange()throws Exception {
        var plan=new ArrayList<String>();
        try(var c=pg.getPostgresDatabase().getConnection();var s=c.createStatement()) {
            s.execute("INSERT INTO orders(id,external_reference,description,status,created_at) SELECT md5('plan'||n)::uuid,'plan-'||n,'CANARY_PAYLOAD','CONFIRMED',now()-interval '2 hours' FROM generate_series(1,100000) n");s.execute("INSERT INTO orders(id,external_reference,description,status,created_at) SELECT md5('recent'||n)::uuid,'recent-'||n,'CANARY_PAYLOAD','CONFIRMED',now()-interval '1 second' FROM generate_series(1,20) n");s.execute("ANALYZE orders");
            try(var r=s.executeQuery("EXPLAIN (ANALYZE, BUFFERS) SELECT id,status,created_at FROM orders WHERE created_at>=now()-interval '5 minutes' AND created_at<now() ORDER BY created_at DESC,id DESC LIMIT 16")) {
                while(r.next()){String line=r.getString(1);if(line.contains("Limit")||line.contains("Index Scan")||line.contains("Index Cond:")||line.contains("Buffers:")||line.contains("Planning Time:")||line.contains("Execution Time:"))plan.add(line);}
            }
        }
        assertThat(String.join("\n",plan)).contains("ops03d_orders_recent","Index Cond:","rows=16");
        var handler=new OrderRecentSearchHandler(pool,env());var selected=query();var page=handler.search(selected);assertThat(page.candidatesExamined()).isEqualTo(16);assertThat(page.items()).hasSize(16);assertThat(page.coverage()).contains(Gap.CANDIDATE_PAGE_LIMIT);
        var seen=new ArrayList<UUID>(page.items().stream().map(Item::ref).toList());
        assertThat(page.nextCursor()).isNotEmpty();
        var continued=new Query(selected.environment(),selected.owner(),selected.kind(),selected.scope(),selected.fromInclusive(),selected.toExclusive(),selected.state(),selected.limit(),page.nextCursor());
        var observed=new java.util.concurrent.atomic.AtomicReference<Page>();
        org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(2)).until(()->{
            try{observed.set(handler.search(continued));return true;}catch(GatewayException expected){if(expected.code()==GatewayException.Code.RESOURCE_EXHAUSTED)return false;throw expected;}
        });
        assertThat(observed.get().items()).hasSize(4);assertThat(observed.get().nextCursor()).isEmpty();seen.addAll(observed.get().items().stream().map(Item::ref).toList());
        var independentlySorted=new ArrayList<UUID>();
        try(var connection=pg.getPostgresDatabase().getConnection();var statement=connection.prepareStatement("SELECT id FROM orders WHERE created_at>=? AND created_at<? ORDER BY created_at DESC,id DESC")){
            statement.setObject(1,selected.fromInclusive().atOffset(ZoneOffset.UTC));statement.setObject(2,selected.toExclusive().atOffset(ZoneOffset.UTC));try(var rows=statement.executeQuery()){while(rows.next())independentlySorted.add(rows.getObject(1,UUID.class));}
        }
        assertThat(seen).containsExactlyElementsOf(independentlySorted).doesNotHaveDuplicates();
        var base=query();var empty=new Query(base.environment(),base.owner(),base.kind(),base.scope(),base.fromInclusive(),base.fromInclusive().plusSeconds(1),"ANY",16,"");
        assertThat(new OrderRecentSearchHandler(pool,env()).search(empty).items()).isEmpty();
        java.nio.file.Files.writeString(java.nio.file.Path.of("target/ops03d-order-cell-plan.txt"),String.join("\n",plan));
    }

    @Test void exhaustedOperationalPoolIsFiniteAndSameSearchHandlerRecovers()throws Exception {
        var h=new OrderRecentSearchHandler(pool,env());var q=query();long started=System.nanoTime();
        try(var held=pool.dataSource().getConnection()) {assertThatThrownBy(()->h.search(q)).isInstanceOf(GatewayException.class);}
        assertThat(Duration.ofNanos(System.nanoTime()-started)).isLessThan(Duration.ofSeconds(1));
        org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(2)).untilAsserted(()->{
            try{assertThat(h.search(q).items()).isEmpty();}catch(GatewayException e){assertThat(e.code()).isEqualTo(GatewayException.Code.RESOURCE_EXHAUSTED);throw new AssertionError("expected admission cooldown",e);}
        });
    }
    Query query(){return new Query("test","order-cell",Kind.WORKFLOW,"order",Instant.now().minusSeconds(300),Instant.now(),"ANY",16,"");}
    @Test void actualOwnerStatesAndPrimaryIndexExposeOnlyCurrentMetadata()throws Exception {
        try(var c=pg.getPostgresDatabase().getConnection()){for(String state:List.of("PENDING_INVENTORY","PENDING_PAYMENT","CANCELLING","CONFIRMED","CANCELLED","CREATED")){var id=UUID.randomUUID();try(var s=c.prepareStatement("INSERT INTO orders(id,external_reference,description,status,created_at) VALUES (?, ?, 'CANARY_PAYLOAD', ?, now()-interval '1 second')")){s.setObject(1,id);s.setString(2,id.toString());s.setString(3,state);s.executeUpdate();}}}
        var page=new OrderRecentSearchHandler(pool,env()).search(query());assertThat(page.items()).extracting(Item::state).contains("PENDING_INVENTORY","PENDING_PAYMENT","CANCELLING","CONFIRMED","CANCELLED","CREATED");
        assertThat(page.coverage()).contains(Gap.PARTIAL_CURRENT_STATE,Gap.RETENTION_UNQUALIFIED);assertThat(page.toString()).doesNotContain("CANARY_","payload","description");
    }
    @Test void insecureAndUnregisteredScopeConfigurationsFailClosed(){
        assertThatThrownBy(()->new OrderRecentSearchHandler(pool,env().withProperty("apex.ops03d.indexes-reviewed","false"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(()->new OrderRecentSearchHandler(pool,env().withProperty("apex.grpc.security.enabled","false"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(()->new OrderRecentSearchHandler(pool,env().withProperty("apex.grpc.max-payload-bytes","65537"))).isInstanceOf(IllegalStateException.class).hasMessage("OPS_SEARCH_WIRE_POLICY");
        assertThatThrownBy(()->new OrderRecentSearchHandler(pool,env().withProperty("apex.ops03d.scopes","WORKFLOW:other"))).isInstanceOf(IllegalArgumentException.class);
        var h=new OrderRecentSearchHandler(pool,env());var q=query();
        assertThatThrownBy(()->h.search(new Query("other",q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),"ANY",16,""))).isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
    }

    @Test void scopedReceiptsAreIndependentOfEventsAndNeverExportIdentityOrResponse() throws Exception {
        try(var c=pg.getPostgresDatabase().getConnection();var statement=c.createStatement()){
            statement.execute("INSERT INTO order_request_receipt(idempotency_key,client_id,tenant_id,principal,contract_type,contract_version,request_id,fingerprint,created_at) SELECT 'old-'||n,'fixture-client','CANARY_TENANT','CANARY_PRINCIPAL','PLACE_ORDER',1,md5(n::text)::uuid,repeat('x',64),now()-interval '2 days' FROM generate_series(1,100000) n");
            statement.execute("ANALYZE order_request_receipt");
            var plan=new StringBuilder();try(var rows=statement.executeQuery("EXPLAIN(ANALYZE,BUFFERS) SELECT request_id,created_at,completed_at FROM order_request_receipt WHERE client_id='fixture-client' AND created_at>=now()-interval '5 minutes' AND created_at<now() ORDER BY created_at DESC,request_id DESC LIMIT 16")){while(rows.next())plan.append(rows.getString(1)).append('\n');}
            assertThat(plan.toString()).contains("ops03d_order_request_receipt_recent","Index Cond:");java.nio.file.Files.writeString(java.nio.file.Path.of("target/ops03d-receipt-plan.txt"),plan);
        }
        var id=UUID.randomUUID();
        try(var c=pg.getPostgresDatabase().getConnection();var s=c.prepareStatement("INSERT INTO order_request_receipt(idempotency_key,client_id,tenant_id,principal,contract_type,contract_version,request_id,fingerprint,response,created_at,completed_at) VALUES ('CANARY_KEY','fixture-client','CANARY_TENANT','CANARY_PRINCIPAL','PLACE_ORDER',1,?,repeat('x',64),decode('43414e415259','hex'),now(),now())")){s.setObject(1,id);s.executeUpdate();}
        try(var c=pg.getPostgresDatabase().getConnection();var statement=c.createStatement()) {
            statement.execute("INSERT INTO order_request_receipt(idempotency_key,client_id,tenant_id,principal,contract_type,contract_version,request_id,fingerprint,created_at) SELECT 'CANARY_PAGE_'||n,'fixture-client','CANARY_TENANT','CANARY_PRINCIPAL','PLACE_ORDER',1,md5('receipt-page'||n)::uuid,repeat('x',64),now()-interval '1 second' FROM generate_series(1,20) n");
            statement.execute("INSERT INTO order_request_receipt(idempotency_key,client_id,tenant_id,principal,contract_type,contract_version,request_id,fingerprint,created_at) VALUES ('CANARY_FOREIGN','other-client','CANARY_TENANT','CANARY_PRINCIPAL','PLACE_ORDER',1,'00000000-0000-0000-0000-000000000001',repeat('x',64),now()-interval '1 second')");
            var plan=new StringBuilder();try(var rows=statement.executeQuery("EXPLAIN(ANALYZE,BUFFERS) SELECT request_id,created_at,completed_at FROM order_request_receipt WHERE client_id='fixture-client' AND created_at>=now()-interval '5 minutes' AND created_at<now() ORDER BY created_at DESC,request_id DESC LIMIT 16")){while(rows.next())plan.append(rows.getString(1)).append('\n');}
            assertThat(plan.toString()).contains("ops03d_order_request_receipt_recent","Index Cond:","rows=16");java.nio.file.Files.writeString(java.nio.file.Path.of("target/ops03d-receipt-matching-plan.txt"),plan);
        }
        var enabled=env().withProperty("apex.ops03d.receipt-client","fixture-client").withProperty("apex.ops03d.scopes","REQUEST_RECEIPT:order-request");
        var h=new OrderRecentSearchHandler(pool,enabled);var q=new Query("test","order-cell",Kind.REQUEST_RECEIPT,"order-request",Instant.now().minusSeconds(30),Instant.now(),"ANY",16,"");
        var page=h.search(q);assertThat(page.items()).extracting(Item::ref).contains(id);assertThat(page.toString()).contains("RECEIPT_COMMITTED").doesNotContain("CANARY_","fixture-client","response=","principal=");
        assertThat(page.items()).hasSize(16);assertThat(page.nextCursor()).isNotEmpty();
        var next=new Query(q.environment(),q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),q.state(),q.limit(),page.nextCursor());
        var continued=new java.util.concurrent.atomic.AtomicReference<Page>();
        org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(2)).until(()->{
            try{continued.set(h.search(next));return true;}catch(GatewayException expected){if(expected.code()==GatewayException.Code.RESOURCE_EXHAUSTED)return false;throw expected;}
        });
        assertThat(continued.get().items()).hasSize(5);assertThat(continued.get().nextCursor()).isEmpty();
        var observed=new ArrayList<UUID>(page.items().stream().map(Item::ref).toList());observed.addAll(continued.get().items().stream().map(Item::ref).toList());
        var expectedOrder=new ArrayList<UUID>();
        try(var c=pg.getPostgresDatabase().getConnection();var statement=c.createStatement();var rows=statement.executeQuery("SELECT request_id FROM order_request_receipt WHERE client_id='fixture-client' AND created_at>=now()-interval '30 seconds' ORDER BY created_at DESC,request_id DESC")){while(rows.next())expectedOrder.add(rows.getObject(1,UUID.class));}
        assertThat(observed).containsExactlyElementsOf(expectedOrder).doesNotHaveDuplicates().doesNotContain(new UUID(0,1));
        assertThatThrownBy(()->new OrderRecentSearchHandler(pool,env().withProperty("apex.ops03d.scopes","REQUEST_RECEIPT:order-request"))).isInstanceOf(IllegalArgumentException.class);
    }

}

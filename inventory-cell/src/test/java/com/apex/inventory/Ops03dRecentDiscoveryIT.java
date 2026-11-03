package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import java.util.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.net.http.HttpClient;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import tools.jackson.databind.node.ObjectNode;

/** Three packaged processes, three owning databases, real Kafka; no cross-Cell production calls. */
@Tag("integration")
class Ops03dRecentDiscoveryIT {
    static final String REQUEST="apex.order.payment-requests.v1";
    static final String RELEASE="apex.order.inventory-release-requests.v1";
    static final String PAYMENT="apex.payment.events.v1";
    @Test void newIdsAreDiscoveredThroughDistinctSearchAclAndRecoverAfterConsoleLoss() throws Exception {
        Path root=Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        Path inventoryJar=Path.of(System.getProperty("saga.inventoryJar")).toAbsolutePath().normalize();
        Path orderJar=root.resolve("order-cell/target/order-cell.jar"),paymentJar=root.resolve("payment-cell/target/payment-cell.jar");
        var build=JSON.readTree(Files.readString(root.resolve("out/ops03d/build-receipt.json")));
        for(String cell:List.of("order-cell","inventory-cell","payment-cell","apex-operations-console")) {
            Path jar=root.resolve(cell+"/target/"+(cell.equals("apex-operations-console")?cell+"-1.0-SNAPSHOT.jar":cell+".jar"));
            assertThat(sha(jar)).isEqualTo(build.path(cell).asText());
        }
        this.root=root;
        Path output=inventoryJar.getParent(); var evidence=new ArrayList<Map<String,Object>>();
        var streams=List.of(ORDER_TOPIC,INVENTORY_TOPIC,REQUEST,RELEASE,PAYMENT,"apex.order.legacy.events.v1");
        var topics=new ArrayList<String>(); for(String s:streams) {topics.add(s);topics.add(s+"-dlt");}
        var broker=new EmbeddedKafkaKraftBroker(1,1,topics.toArray(String[]::new));
        broker.brokerProperty("auto.create.topics.enable","false");broker.brokerListProperty("apex.m27.test.brokers");
        Process order=null,inventory=null,payment=null;

        try(var ownedCerts=new Ops03bCertificates();var orderDb=EmbeddedPostgres.builder().start();var invDb=EmbeddedPostgres.builder().start();
            var payDb=EmbeddedPostgres.builder().start();var http=HttpClient.newHttpClient()) {
            this.certs=ownedCerts;this.apiClient=http;
            try {
                broker.afterPropertiesSet();String brokers=broker.getBrokersAsString();
                String og="order-three-"+UUID.randomUUID(),ig="inventory-three-"+UUID.randomUUID(),pg="payment-three-"+UUID.randomUUID();
                int op=freePort(),ip=freePort();
                Path ol=output.resolve("three-order.log"),il=output.resolve("three-inventory.log"),pl=output.resolve("three-payment.log");
                order=launch(orderJar,orderDb,brokers,og,op,ol,ORDER_TOPIC,INVENTORY_TOPIC+","+PAYMENT,
                        "--spring.profiles.active=saga,three-cell-saga");
                inventory=launch(inventoryJar,invDb,brokers,ig,ip,il,INVENTORY_TOPIC,ORDER_TOPIC+","+RELEASE,
                        "--spring.profiles.active=saga,three-cell-saga",
                        "--apex.messaging.consumer.reliability.max-attempts=10");
                payment=launch(paymentJar,payDb,brokers,pg,0,pl,PAYMENT,REQUEST,
                        "--spring.profiles.active=three-cell-saga,simulation",
                        "--apex.messaging.consumer.reliability.max-attempts=10");
                ready(order,ol,http,op);ready(inventory,il,http,ip);paymentReady(payment,pl);
                var orders=new JdbcTemplate(orderDb.getPostgresDatabase());
                var stock=new JdbcTemplate(invDb.getPostgresDatabase());var pay=new JdbcTemplate(payDb.getPostgresDatabase());
                assertThat(orders.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'",String.class)).doesNotContain("inventory_stock","payment_authorizations");
                assertThat(stock.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'",String.class)).doesNotContain("orders","payment_authorizations");
                assertThat(pay.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'",String.class)).doesNotContain("orders","inventory_stock");
                for(var jdbc:List.of(orders,stock,pay)) {grantReader(jdbc);prepareRetained(jdbc);}
                startSearchConsole(certs.primary,"m32-client"); // starts before any business UUID exists
                assertThat(JSON.readTree(api("/ops/v1/events/00000000-0000-0000-0000-000000000001")).path("code").asText()).isEqualTo("SCOPE_DENIED");
                evidence.add(Map.of("stage","started","orderPid",order.pid(),"inventoryPid",inventory.pid(),"paymentPid",payment.pid(),
                        "orderGroup",og,"inventoryGroup",ig,"paymentGroup",pg,"brokers",brokers,"topics",topics));
                assertThat(new HashSet<>(List.of(order.pid(),inventory.pid(),payment.pid(),ProcessHandle.current().pid()))).hasSize(4);
                try(var admin=Admin.create(Map.of("bootstrap.servers",brokers));var wire=observer(brokers);var replay=producer(brokers)) {
                    wire.assign(streams.stream().map(s->new TopicPartition(s,0)).toList());wire.seekToBeginning(wire.assignment());
                    var cache=new HashMap<UUID,ConsumerRecord<String,String>>();
                    UUID a=UUID.randomUUID(),b=UUID.randomUUID();
                    stock.update("INSERT INTO inventory_stock VALUES (?,10,0),(?,10,0)",a,b);
                    orders.execute("CREATE TABLE ops03b_mark_gate(enabled boolean NOT NULL)");orders.update("INSERT INTO ops03b_mark_gate VALUES(true)");
                    orders.execute("CREATE FUNCTION ops03b_mark_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.published AND NEW.event_type='ORDER_PLACED' AND (SELECT enabled FROM ops03b_mark_gate) THEN RAISE EXCEPTION 'owned publication mark fault'; END IF; RETURN NEW; END $$");
                    orders.execute("CREATE CONSTRAINT TRIGGER ops03b_mark_fail AFTER UPDATE ON outbox_event DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ops03b_mark_fail()");
                    long firstPaymentPid=payment.pid();stop(payment);payment=null;
                    UUID happy=UUID.fromString(postCanary(http,op,List.of(item(a,2))).path("orderId").asText());lastHappy=happy;
                    awaitStatus(orders,happy,"PENDING_PAYMENT");
                    assertThat(count(pay,"payment_authorizations","order_id",happy)).isZero();
                    pay.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')",happy);
                    var gapEvent=typed(orders,happy,"ORDER_PLACED");UUID gapId=(UUID)gapEvent.get("event_id");
                    var gapWire=observe(wire,cache,gapId);assertEnvelope(orders,gapId,gapWire);
                    assertThat(orders.queryForObject("SELECT published FROM outbox_event WHERE event_id=?",Boolean.class,gapId)).isFalse();
                    watch(orders,stock,pay,happy,"PENDING_PAYMENT","RESERVED",null,List.of());
                    await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(20)).untilAsserted(()->{
                        var gapView=JSON.readTree(search("/ops/v1/recent/events","order-cell","owner"));var gapStates=new ArrayList<String>();
                        assertThat(gapView.path("freshness").asText()).isEqualTo("FRESH");
                        for(var item:gapView.path("page").path("items"))if(item.path("ref").asText().equals(gapId.toString()))gapStates.add(item.path("state").asText());
                        assertThat(gapStates).hasSize(1).doesNotContain("PUBLISHER_MARK_COMMITTED");assertThat(gapView.path("brokerAck").asText()).isEqualTo("UNKNOWN_NO_VERIFIED_RECEIPT");
                    });
                    orders.update("UPDATE ops03b_mark_gate SET enabled=false");
                    var retryAt=orders.queryForObject("SELECT next_retry_at FROM outbox_event WHERE event_id=?",java.time.OffsetDateTime.class,gapId);
                    int retries=orders.queryForObject("SELECT retry_count FROM outbox_event WHERE event_id=?",Integer.class,gapId);
                    assertThat(retries).isEqualTo(1);assertThat(retryAt).isNotNull();
                    Files.writeString(root.resolve("out/ops03d/mark-gap.json"),JSON.writeValueAsString(Map.of("eventId",gapId,"brokerOffset",gapWire.offset(),"retryCount",retries,"retryDueAt",retryAt.toInstant().toString(),"markCommitted",false,"consumerInboxCommitted",true)));
                    await().atMost(Duration.ofSeconds(35)).until(()->!java.time.Instant.now().isBefore(retryAt.toInstant()));
                    awaitPublished(orders,gapId);
                    payment=launch(paymentJar,payDb,brokers,pg,0,output.resolve("three-payment-restarted.log"),PAYMENT,REQUEST,
                            "--spring.profiles.active=three-cell-saga,simulation","--apex.messaging.consumer.reliability.max-attempts=10");
                    paymentReady(payment,output.resolve("three-payment-restarted.log"));awaitStatus(orders,happy,"CONFIRMED");
                    assertThat(getStatus(http,op,happy)).isEqualTo("CONFIRMED");assertStock(stock,a,8,2);
                    assertThat(pay.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?",String.class,happy)).isEqualTo("AUTHORIZED");
                    assertThat(count(pay,"outbox_event","aggregate_id",happy)).isEqualTo(1);
                    var ready=typed(orders,happy,"ORDER_READY_FOR_PAYMENT");
                    var placed=typed(orders,happy,"ORDER_PLACED");
                    var reserved=typed(stock,happy,"INVENTORY_RESERVED");var authorized=typed(pay,happy,"PAYMENT_AUTHORIZED");
                    assertLink(placed,reserved);assertLink(reserved,ready);assertLink(ready,authorized);
                    for(var event:List.of(placed,reserved,ready,authorized)) {
                        UUID id=(UUID)event.get("event_id");var record=observe(wire,cache,id);
                        assertThat(record.topic()).isEqualTo(switch(event.get("event_type").toString()) {
                            case "ORDER_PLACED"->ORDER_TOPIC;case "ORDER_READY_FOR_PAYMENT"->REQUEST;
                            case "INVENTORY_RESERVED"->INVENTORY_TOPIC;default->PAYMENT;});
                        assertThat(record.key()).isEqualTo(event.get("aggregate_type")+":"+happy);
                        JdbcTemplate owner=switch(event.get("event_type").toString()) {
                            case "INVENTORY_RESERVED"->stock;case "PAYMENT_AUTHORIZED"->pay;default->orders;};
                        assertEnvelope(owner,id,record);
                        evidence.add(wireEvidence(record,id));
                    }
                    assertThat(count(stock,"processed_event","event_id",(UUID)placed.get("event_id"))).isEqualTo(1);
                    assertThat(count(pay,"processed_event","event_id",(UUID)ready.get("event_id"))).isEqualTo(1);
                    assertThat(count(orders,"processed_event","event_id",(UUID)authorized.get("event_id"))).isEqualTo(1);
                    // Inbox duplicate and business-key duplicate on all three consumers.
                    for(var event:List.of(reserved,ready,authorized,placed)) {
                        var original=observe(wire,cache,(UUID)event.get("event_id"));
                        replay.send(new ProducerRecord<>(original.topic(),0,original.key(),original.value())).get(10,TimeUnit.SECONDS);
                        ObjectNode changed=(ObjectNode)JSON.readTree(original.value());
                        ((ObjectNode)changed.get("metadata")).put("eventId",UUID.randomUUID().toString());
                        var last=replay.send(new ProducerRecord<>(original.topic(),0,original.key(),JSON.writeValueAsString(changed))).get(10,TimeUnit.SECONDS);
                        awaitConsumed(admin,original.topic().equals(REQUEST)?pg:original.topic().equals(ORDER_TOPIC)?ig:og,original.topic(),last.offset());
                    }
                    assertThat(count(pay,"payment_authorizations","order_id",happy)).isEqualTo(1);
                    assertThat(count(pay,"outbox_event","aggregate_id",happy)).isEqualTo(1);
                    assertThat(orders.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type='ORDER_READY_FOR_PAYMENT'",Long.class,happy)).isEqualTo(1);
                    assertStock(stock,a,8,2);assertThat(status(orders,happy)).isEqualTo("CONFIRMED");
                    watch(orders,stock,pay,happy,"CONFIRMED","RESERVED","AUTHORIZED",List.of());
                    String paymentPath=searchPath("/ops/v1/recent/workflows","payment-cell","authorization-simulation");
                    await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(20)).untilAsserted(()->assertThat(JSON.readTree(api(paymentPath)).path("page").path("items").toString()).contains("AUTHORIZED"));
                    String epochBefore=JSON.readTree(api(paymentPath)).path("page").path("epoch").asText();
                    assertThat(epochBefore).isNotBlank();stop(payment);payment=null;
                    await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(25)).untilAsserted(()->{
                        var stale=JSON.readTree(api(paymentPath));
                        assertThat(stale.path("freshness").asText()).isEqualTo("STALE");assertThat(stale.path("page").toString()).contains("AUTHORIZED");
                    });
                    payment=launch(paymentJar,payDb,brokers,pg,0,output.resolve("ops03d-payment-recovered.log"),PAYMENT,REQUEST,"--spring.profiles.active=three-cell-saga,simulation","--apex.messaging.consumer.reliability.max-attempts=10");
                    paymentReady(payment,output.resolve("ops03d-payment-recovered.log"));
                    await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(45)).untilAsserted(()->{
                        var fresh=JSON.readTree(api(paymentPath));assertThat(fresh.path("freshness").asText()).isEqualTo("FRESH");assertThat(fresh.path("page").path("epoch").asText()).isNotEqualTo(epochBefore);
                    });
                    assertNoDlt(admin,streams);
                    evidence.add(Map.of("stage","happy","orderId",happy,"status","CONFIRMED","paymentPidBefore",firstPaymentPid,
                            "paymentPidAfter",payment.pid(),"eventId",placed.get("event_id"),"correlationId",placed.get("correlation_id"),"stock",List.of(8,2)));
                    // A real deferred COMMIT failure during compensation: CANCELLING is observable.
                    stock.execute("CREATE TABLE release_failure_gate(enabled boolean NOT NULL)");stock.update("INSERT INTO release_failure_gate VALUES(true)");
                    stock.execute("CREATE SEQUENCE release_failure_attempt");
                    stock.execute("CREATE FUNCTION fail_release_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                            +"IF NEW.event_type='ORDER_CANCELLATION_REQUESTED' AND (SELECT enabled FROM release_failure_gate) THEN "
                            +"PERFORM nextval('release_failure_attempt');PERFORM pg_sleep(1);RAISE EXCEPTION 'test release commit failure'; END IF;RETURN NEW;END $$");
                    stock.execute("CREATE CONSTRAINT TRIGGER fail_release_commit AFTER INSERT ON processed_event DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_release_commit()");
                    UUID declined=UUID.fromString(post(http,op,List.of(item(b,3))).path("orderId").asText());
                    pay.update("INSERT INTO payment_simulation_policy VALUES (?,'DECLINED')",declined);
                    awaitStatus(orders,declined,"CANCELLING");
                    await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertThat(stock.queryForObject("SELECT last_value FROM release_failure_attempt",Long.class)).isGreaterThanOrEqualTo(2));
                    assertStock(stock,b,7,3);
                    assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,declined)).isEqualTo("RESERVED");
                    var cancellation=typed(orders,declined,"ORDER_CANCELLATION_REQUESTED");
                    var failedWire=observe(wire,cache,(UUID)cancellation.get("event_id"));
                    assertThat(count(stock,"processed_event","event_id",(UUID)cancellation.get("event_id"))).isZero();
                    assertThat(stock.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type='INVENTORY_RELEASED'",Long.class,declined)).isZero();
                    var offset=admin.listConsumerGroupOffsets(ig).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS).get(new TopicPartition(RELEASE,0));
                    assertThat(offset==null || offset.offset()<=failedWire.offset()).isTrue();
                    evidence.add(Map.of("stage","release-commit-rollback","sourceTopic",RELEASE,"sourcePartition",0,
                            "sourceOffset",failedWire.offset(),"committedOffset",offset==null?-1:offset.offset(),
                            "inboxRows",0,"releaseOutboxRows",0,"stock",List.of(7,3)));
                    assertThat(getStatus(http,op,declined)).isEqualTo("CANCELLING");
                    watch(orders,stock,pay,declined,"CANCELLING","RESERVED","DECLINED",List.of());
                    stock.update("UPDATE release_failure_gate SET enabled=false");awaitStatus(orders,declined,"CANCELLED");
                    assertThat(getStatus(http,op,declined)).isEqualTo("CANCELLED");assertStock(stock,b,10,0);
                    assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,declined)).isEqualTo("RELEASED");
                    watch(orders,stock,pay,declined,"CANCELLED","RELEASED","DECLINED",List.of());
                    var declinedPayment=typed(pay,declined,"PAYMENT_DECLINED");var released=typed(stock,declined,"INVENTORY_RELEASED");
                    assertLink(declinedPayment,cancellation);assertLink(cancellation,released);
                    for(var event:List.of(declinedPayment,cancellation,released)) {
                        UUID id=(UUID)event.get("event_id");var record=observe(wire,cache,id);
                        JdbcTemplate owner=switch(event.get("event_type").toString()) {
                            case "INVENTORY_RELEASED"->stock;case "PAYMENT_DECLINED"->pay;default->orders;};
                        assertEnvelope(owner,id,record);evidence.add(wireEvidence(record,id));
                    }
                    // Distinct-event duplicate release re-acknowledges liveness; stock and Order never change again.
                    var releaseWire=observe(wire,cache,(UUID)cancellation.get("event_id"));
                    for(int i=0;i<2;i++) {
                        ObjectNode copy=(ObjectNode)JSON.readTree(releaseWire.value());
                        if(i==1) ((ObjectNode)copy.get("metadata")).put("eventId",UUID.randomUUID().toString());
                        var sent=replay.send(new ProducerRecord<>(RELEASE,0,releaseWire.key(),JSON.writeValueAsString(copy))).get(10,TimeUnit.SECONDS);
                        awaitConsumed(admin,ig,RELEASE,sent.offset());
                    }
                    assertStock(stock,b,10,0);
                    assertThat(count(pay,"payment_authorizations","order_id",declined)).isEqualTo(1);
                    assertThat(orders.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type='ORDER_CANCELLATION_REQUESTED'",Long.class,declined)).isEqualTo(1);
                    assertThat(status(orders,declined)).isEqualTo("CANCELLED");
                    evidence.add(Map.of("stage","declined-compensated","orderId",declined,"intermediate","CANCELLING","status","CANCELLED",
                            "releaseEventId",released.get("event_id"),"correlationId",released.get("correlation_id"),"stock",List.of(10,0),
                            "rollbackAndNoAckVerified",true));
                    UUID rejected=UUID.fromString(post(http,op,List.of(item(b,100))).path("orderId").asText());
                    awaitStatus(orders,rejected,"CANCELLED");assertThat(getStatus(http,op,rejected)).isEqualTo("CANCELLED");
                    assertStock(stock,b,10,0);assertThat(count(pay,"payment_authorizations","order_id",rejected)).isZero();
                    assertThat(orders.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type IN ('ORDER_READY_FOR_PAYMENT','ORDER_CANCELLATION_REQUESTED')",Long.class,rejected)).isZero();
                    assertThat(stock.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type='INVENTORY_RELEASED'",Long.class,rejected)).isZero();
                    assertNoDlt(admin,streams);
                    watch(orders,stock,pay,rejected,"CANCELLED","REJECTED","UNKNOWN",List.of());
                    evidence.add(Map.of("stage","out-of-stock","orderId",rejected,"status","CANCELLED","paymentAttempts",0,"releaseRequests",0));
                    // A contradictory outcome is a technical/contract failure, never a terminal-state overwrite.
                    ObjectNode contradiction=(ObjectNode)JSON.readTree(observe(wire,cache,(UUID)authorized.get("event_id")).value());
                    UUID contradictoryId=UUID.randomUUID();
                    ((ObjectNode)contradiction.get("metadata")).put("eventId",contradictoryId.toString());
                    ((ObjectNode)contradiction.get("metadata")).put("eventType","PAYMENT_DECLINED");
                    String contradictoryRaw=JSON.writeValueAsString(contradiction);
                    var sentContradiction=replay.send(new ProducerRecord<>(PAYMENT,0,"PAYMENT_AUTHORIZATION:"+happy,contradictoryRaw)).get(10,TimeUnit.SECONDS);
                    try(var quarantine=observer(brokers)) {
                        quarantine.assign(List.of(new TopicPartition(PAYMENT+"-dlt",0)));
                        quarantine.seekToBeginning(quarantine.assignment());
                        var quarantined=observe(quarantine,new HashMap<>(),contradictoryId);
                        assertQuarantine(quarantined,contradictoryRaw,PAYMENT,sentContradiction.offset(),og,"IllegalStateException");
                        awaitConsumed(admin,og,PAYMENT,sentContradiction.offset());
                        assertThat(count(orders,"processed_event","event_id",contradictoryId)).isZero();
                        assertThat(status(orders,happy)).isEqualTo("CONFIRMED");assertStock(stock,a,8,2);
                        assertThat(orders.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type='ORDER_CANCELLATION_REQUESTED'",Long.class,happy)).isZero();
                        watch(orders,stock,pay,happy,"CONFIRMED","RESERVED","AUTHORIZED",List.of(contradictoryId));
                        assertThat(search("/ops/v1/recent/events","order-cell","owner")).contains("UNKNOWN_NOT_INSPECTED");
                        evidence.add(Map.of("stage","contradictory-payment-dlt","eventId",contradictoryId,
                                "sourceOffset",sentContradiction.offset(),"dltOffset",quarantined.offset(),
                                "dltTopic",quarantined.topic(),"orderStatus","CONFIRMED","inboxRows",0));
                    }
                }
                smokeScript();
                securityNegatives();
                // Console loss has no business dependency; restart has no persisted cache to pretend is fresh.
                stop(console);console=null;stop(payment);payment=null;
                UUID afterLoss=UUID.fromString(post(http,op,List.of(item(UUID.randomUUID(),1))).path("orderId").asText());
                awaitStatus(orders,afterLoss,"CANCELLED");
                startSearchConsole(certs.primary,"m32-client");
                assertWorkflow("order-cell","order",afterLoss,"CANCELLED");
                var missing=JSON.readTree(search("/ops/v1/recent/workflows","payment-cell","authorization-simulation"));
                assertThat(missing.path("page").isNull()).isTrue();assertThat(missing.path("freshness").asText()).isEqualTo("MISSING");
                payment=launch(paymentJar,payDb,brokers,pg,0,output.resolve("ops03d-payment-final.log"),PAYMENT,REQUEST,"--spring.profiles.active=three-cell-saga,simulation","--apex.messaging.consumer.reliability.max-attempts=10");
                paymentReady(payment,output.resolve("ops03d-payment-final.log"));
                assertWorkflow("payment-cell","authorization-simulation",lastHappy,"AUTHORIZED");
                evidence.add(Map.of("stage","console-loss-and-restart","newOrderId",afterLoss,"status","CANCELLED","projection","BLOCKED_EPHEMERAL_REREAD_ONLY"));
                Files.writeString(output.resolve("ops03d-discovery-evidence.json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
            } finally {try {stop(console);}finally{try{stop(payment);}finally{try{stop(inventory);}finally{try{stop(order);}finally{broker.destroy();if(certs!=null)certs.close();
                Files.writeString(root.resolve("out/ops03d/cleanup.json"),JSON.writeValueAsString(Map.of("children",children.stream().map(Process::pid).toList(),"allStopped",children.stream().noneMatch(Process::isAlive))));
                scanPrivacy(output);
                }}}}}
        }
    }
    Path root;java.net.http.HttpClient apiClient;Ops03bCertificates certs;Process console;int consolePort;int sequence;
    UUID lastHappy;final java.time.Instant windowStart=java.time.Instant.now().minusSeconds(5);final Map<String,String> groups=new LinkedHashMap<>();
    final Map<String,Integer> ports=new LinkedHashMap<>();final List<Process> children=new ArrayList<>();
    Process launch(Path jar,EmbeddedPostgres db,String brokers,String group,int port,Path log,String publish,String subscribe,String... overrides)throws Exception {
        String cell=jar.getFileName().toString().replace(".jar","");groups.put(cell,group);int grpc=ports.computeIfAbsent(cell,k->{try{return freePort();}catch(Exception e){throw new IllegalStateException(e);}});
        var args=new ArrayList<String>(Arrays.asList(overrides));args.addAll(certs.bundle(certs.server));
        args.addAll(List.of("--apex.grpc.cell-id="+cell,"--apex.grpc.server.enabled=true","--spring.grpc.server.address=127.0.0.1","--spring.grpc.server.port="+grpc,
            "--spring.grpc.server.ssl.enabled=true","--spring.grpc.server.ssl.bundle=m32","--spring.grpc.server.ssl.client-auth=require","--spring.grpc.server.reflection.enabled=false",
            "--apex.grpc.security.enabled=true","--apex.grpc.security.tenant=alpha","--apex.grpc.security.peers.ops.uri=spiffe://apex.test/tenant/alpha/cell/m32-client","--apex.grpc.security.peers.ops.contracts=QUERY:OPS_OWNER_SNAPSHOT:1,QUERY:OPS_OWNER_RECENT_SEARCH:1",
            "--apex.grpc.security.peers.observer.uri=spiffe://apex.test/tenant/alpha/cell/m32-observer","--apex.grpc.security.peers.observer.contracts=QUERY:OPS_OWNER_SNAPSHOT:1,QUERY:OPS_OWNER_SEARCH:1",
            "--apex.ops03d.search-enabled=true","--apex.ops03d.indexes-reviewed=true","--apex.ops03d.scopes=OUTBOX_EVENT:owner,WORKFLOW:"+(cell.equals("order-cell")?"order":cell.equals("inventory-cell")?"reservation":"authorization-simulation")+",KNOWN_GROUP_INBOX:"+group,
            "--apex.ops03.owner-enabled=true","--apex.ops03.environment=test","--apex.ops03.instance=one","--apex.ops03.jdbc-url="+db.getJdbcUrl("ops03d_reader","postgres"),"--apex.ops03.jdbc-user=ops03d_reader","--apex.ops03.jdbc-password=","--apex.ops03.known-groups="+group,
            "--apex.observability.enabled=true","--apex.observability.active-enabled=true"));
        var child=OrderInventorySagaIT.launch(jar,db,brokers,group,port,log,publish,subscribe,args.toArray(String[]::new));children.add(child);return child;
    }
    String api(String path)throws Exception {return apiClient.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+consolePort+path)).timeout(Duration.ofSeconds(2)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString()).body();}
    void watch(JdbcTemplate orders,JdbcTemplate stock,JdbcTemplate pay,UUID id,String orderState,String inventoryState,String paymentState,List<UUID> extra)throws Exception {
        String sample=assertWorkflow("order-cell","order",id,orderState);assertWorkflow("inventory-cell","reservation",id,inventoryState);
        if(paymentState!=null && !paymentState.equals("UNKNOWN"))assertWorkflow("payment-cell","authorization-simulation",id,paymentState);
        else if(paymentState!=null) {
            var page=JSON.readTree(search("/ops/v1/recent/workflows","payment-cell","authorization-simulation"));
            assertThat(page.toString()).contains("RETENTION_UNQUALIFIED");assertThat(page.path("page").path("items").toString()).doesNotContain(id.toString());
        }
        for(var entry:Map.of("order-cell",orders,"inventory-cell",stock,"payment-cell",pay).entrySet()) {
            var ids=entry.getValue().queryForList("SELECT event_id FROM outbox_event WHERE aggregate_id=?",UUID.class,id);
            if(!ids.isEmpty()) {
                await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(20)).untilAsserted(()->{
                    var body=JSON.readTree(search("/ops/v1/recent/events",entry.getKey(),"owner"));assertThat(body.path("freshness").asText()).isEqualTo("FRESH");for(UUID event:ids)assertThat(body.path("page").path("items").toString()).contains(event.toString());
                });
            }
        }
        assertThat(sample).doesNotContain("CANARY_","payload","idempotency_key",certs.password);
        Files.writeString(root.resolve("out/ops03d/api-"+(++sequence)+".json"),sample);
    }
    String assertWorkflow(String owner,String scope,UUID ref,String state) {
        var observed=new java.util.concurrent.atomic.AtomicReference<String>();
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(45)).untilAsserted(()->{
            String body=search("/ops/v1/recent/workflows",owner,scope);var value=JSON.readTree(body);assertThat(value.path("freshness").asText()).isEqualTo("FRESH");
            var found=new ArrayList<String>();for(var item:value.path("page").path("items"))if(item.path("ref").asText().equals(ref.toString()))found.add(item.path("state").asText());
            assertThat(found).containsExactly(state);assertThat(value.toString()).contains("RELATION_UNKNOWN","PARTIAL_CURRENT_STATE");observed.set(body);
        });
        return observed.get();
    }
    static tools.jackson.databind.JsonNode postCanary(java.net.http.HttpClient http,int port,List<Map<String,Object>> items)throws Exception {
        String body=JSON.writeValueAsString(Map.of("externalReference","CANARY_EXTERNAL_"+UUID.randomUUID(),"description",PERSIAN+" CANARY_OPS03D_PAYLOAD","items",items));
        var response=http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+port+"/api/v1/order-placements")).timeout(Duration.ofSeconds(10))
            .header("Content-Type","application/json").header("Authorization","Bearer CANARY_OPS03D_TOKEN").POST(java.net.http.HttpRequest.BodyPublishers.ofString(body,StandardCharsets.UTF_8)).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(201);var json=JSON.readTree(response.body());assertThat(json.path("status").asText()).isEqualTo("PENDING_INVENTORY");return json;
    }
    void scanPrivacy(Path output)throws Exception {
        var logs=new ArrayList<Path>();try(var files=Files.list(output)){logs.addAll(files.filter(p->p.getFileName().toString().matches("(three-.*|ops03d-payment.*)\\.log")).toList());}
        try(var files=Files.list(root.resolve("out/ops03d"))){logs.addAll(files.filter(p->p.getFileName().toString().startsWith("console-")&&p.toString().endsWith(".log")).toList());}
        var matches=new ArrayList<String>();for(var file:logs){String text=Files.readString(file);if(text.contains("CANARY_RETAINED_")||text.contains("CANARY_OPS03D_PAYLOAD")||text.contains("CANARY_OPS03D_TOKEN")||text.contains("CANARY_EXTERNAL_")||text.contains(certs.password))matches.add(file.getFileName().toString());}
        Files.writeString(root.resolve("out/ops03d/privacy.json"),JSON.writeValueAsString(Map.of("applicationLogsChecked",logs.size(),"matches",matches,"scope","Owned JVM logs only; native intentional-failure SQL logs excluded")));
        assertThat(matches).isEmpty();
    }
    String searchPath(String route,String owner,String scope) {
        return route+"?environment=test&owner="+owner+"&scope="+scope+"&from="+windowStart+"&to="+java.time.Instant.now()+"&limit=16";
    }
    String search(String route,String owner,String scope)throws Exception {return api(searchPath(route,owner,scope));}
    void startSearchConsole(Path keys,String identity)throws Exception {
        consolePort=freePort();var grants=new ArrayList<String>();
        for(String cell:ports.keySet()) {
            grants.add(cell+"/WORKFLOW/"+(cell.equals("order-cell")?"order":cell.equals("inventory-cell")?"reservation":"authorization-simulation"));
            grants.add(cell+"/OUTBOX_EVENT/owner");grants.add(cell+"/KNOWN_GROUP_INBOX/"+groups.get(cell));
        }
        var args=new ArrayList<String>(List.of("--server.port="+consolePort,"--apex.grpc.cell-id="+identity,"--apex.grpc.client.enabled=true",
            "--apex.ops03d.search-enabled=true","--apex.ops03d.environment=test","--apex.ops03d.sources="+String.join(",",grants)));
        for(var entry:ports.entrySet())args.addAll(List.of("--apex.grpc.destinations."+entry.getKey()+".channel="+entry.getKey(),"--spring.grpc.client.channel."+entry.getKey()+".target=static://127.0.0.1:"+entry.getValue(),"--spring.grpc.client.channel."+entry.getKey()+".ssl.enabled=true","--spring.grpc.client.channel."+entry.getKey()+".ssl.bundle=m32"));
        args.addAll(certs.bundle(keys));
        var command=new ArrayList<String>(List.of(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx256m","-jar",root.resolve("apex-operations-console/target/apex-operations-console-1.0-SNAPSHOT.jar").toString()));command.addAll(args);
        var builder=new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(root.resolve("out/ops03d/console-"+UUID.randomUUID()+".log").toFile());
        builder.environment().keySet().removeIf(k->k.matches("(?i)(SPRING_.*|APEX_.*|CELL_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        console=builder.start();children.add(console);
        await().ignoreExceptions().atMost(Duration.ofSeconds(45)).untilAsserted(()->{assertThat(console.isAlive()).isTrue();assertThat(api("/ops/v1/recent/search/sources")).contains("OUTBOX_EVENT");});
    }
    static void grantReader(JdbcTemplate db) {
        db.execute("CREATE ROLE ops03d_reader LOGIN");db.execute("GRANT USAGE ON SCHEMA public TO ops03d_reader");
        db.execute("GRANT SELECT(id,event_id,published,dead_letter,next_retry_at,claimed_until,created_at,occurred_at) ON outbox_event TO ops03d_reader");
        db.execute("GRANT SELECT(consumer_group,event_id,processed_at) ON processed_event TO ops03d_reader");
        if(db.queryForObject("SELECT to_regclass('orders') IS NOT NULL",Boolean.class))db.execute("GRANT SELECT(id,status,created_at,status_entered_at) ON orders TO ops03d_reader");
        if(db.queryForObject("SELECT to_regclass('inventory_reservations') IS NOT NULL",Boolean.class))db.execute("GRANT SELECT(order_id,status,created_at) ON inventory_reservations TO ops03d_reader");
        if(db.queryForObject("SELECT to_regclass('payment_authorizations') IS NOT NULL",Boolean.class))db.execute("GRANT SELECT(order_id,status,created_at) ON payment_authorizations TO ops03d_reader");
    }
    void prepareRetained(JdbcTemplate db)throws Exception {
        String cell=db.queryForObject("SELECT to_regclass('orders') IS NOT NULL",Boolean.class)?"order-cell":db.queryForObject("SELECT to_regclass('inventory_reservations') IS NOT NULL",Boolean.class)?"inventory-cell":"payment-cell";
        String ddl=Files.readString(root.resolve(cell+"/src/main/resources/db/ops03d/recent-indexes.sql")).replaceAll("(?m)--.*$","");
        try(var connection=db.getDataSource().getConnection();var statement=connection.createStatement()) {
            assertThat(connection.getAutoCommit()).isTrue();
            for(String command:ddl.split(";"))if(!command.isBlank())statement.execute(command);
        } // same owned autocommit session: SET limits govern each concurrent build
        int rows=100000;
        db.execute("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload,published,created_at) SELECT md5('retained-event'||n)::uuid,'TEST',1,md5(n::text)::uuid,md5('retained-aggregate'||n)::uuid,'TEST','fixture','fixture',now()-interval '2 days','{\"private\":\"CANARY_RETAINED_PAYLOAD\"}',true,now()-interval '2 days' FROM generate_series(1,"+rows+") n");
        db.execute("INSERT INTO processed_event SELECT '"+groups.get(cell)+"',md5(n::text)::uuid,'TEST',1,'test',0,n,now()-interval '2 days' FROM generate_series(1,"+rows+") n");
        if(cell.equals("order-cell"))db.execute("INSERT INTO orders(id,external_reference,description,status,created_at) SELECT md5('retained'||n)::uuid,'retained-'||n,'CANARY_RETAINED_DESCRIPTION','CONFIRMED',now()-interval '2 days' FROM generate_series(1,"+rows+") n");
        else db.execute("INSERT INTO "+(cell.equals("inventory-cell")?"inventory_reservations":"payment_authorizations")+"(order_id,status,created_at) SELECT md5('retained'||n)::uuid,'"+(cell.equals("inventory-cell")?"RELEASED":"AUTHORIZED")+"',now()-interval '2 days' FROM generate_series(1,"+rows+") n");
        for(String table:List.of("outbox_event","processed_event",cell.equals("order-cell")?"orders":cell.equals("inventory-cell")?"inventory_reservations":"payment_authorizations"))db.execute("ANALYZE "+table);
        Files.writeString(root.resolve("out/ops03d/retained-"+cell+".json"),JSON.writeValueAsString(Map.of("owner",cell,"oldOutbox",rows,"oldInbox",rows,"oldWorkflow",rows,"newIdsConfigured",false,"ddl","manual concurrent scripts, owner fixture only")));
    }
    void smokeScript()throws Exception {
        Path log=root.resolve("out/ops03d/smoke-output.txt");
        var child=new ProcessBuilder("powershell.exe","-NoProfile","-ExecutionPolicy","Bypass","-File",root.resolve("tools/ops03d/smoke-read-only.ps1").toString(),
                "-Port",Integer.toString(consolePort),"-Owner","order-cell","-Kind","workflows","-Scope","order","-Cycles","2","-MaxPages","2")
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();children.add(child);
        try {
            assertThat(child.waitFor(15,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isZero();
            String text=Files.readString(log);assertThat(text.length()).isLessThan(65536);
            assertThat(text).contains("OWNER_DIRECT","PARTIAL_CURRENT_STATE","RELATION_UNKNOWN","Distinct observed metadata identities:").doesNotContain("CANARY_");
        }finally{stop(child);}
    }
    void securityNegatives()throws Exception {
        assertThat(JSON.readTree(api("/ops/v1/recent/search/openapi")).path("openapi").asText()).isEqualTo("3.1.0");
        for(String suffix:List.of("&limit=1000","&sql=CANARY_SECRET","&cursor="+"a".repeat(257),"&owner=payment-cell")) {
            var response=apiClient.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+consolePort+searchPath("/ops/v1/recent/events","order-cell","owner")+suffix)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(400);assertThat(response.body()).doesNotContain("CANARY_SECRET");
        }
        var mutation=apiClient.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+consolePort+"/ops/v1/recent/events")).POST(java.net.http.HttpRequest.BodyPublishers.noBody()).build(),java.net.http.HttpResponse.BodyHandlers.ofString());assertThat(mutation.statusCode()).isEqualTo(405);
        for(String method:List.of("PUT","PATCH","DELETE")) {
            var denied=apiClient.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+consolePort+"/ops/v1/recent/workflows")).method(method,java.net.http.HttpRequest.BodyPublishers.noBody()).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(denied.statusCode()).isEqualTo(405);
        }
        var body=apiClient.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+consolePort+searchPath("/ops/v1/recent/events","order-cell","owner"))).method("GET",java.net.http.HttpRequest.BodyPublishers.ofString("CANARY_BODY")).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(body.statusCode()).isEqualTo(400);assertThat(body.body()).doesNotContain("CANARY_BODY");
        stop(console);console=null;
        for(var keys:List.of(certs.observer,certs.wrongSan)) {
            startSearchConsole(keys,"m32-observer");
            var denied=JSON.readTree(search("/ops/v1/recent/workflows","order-cell","order"));assertThat(denied.path("page").isNull()).isTrue();
            assertThat(denied.toString()).doesNotContain("OWNER_COMMITTED_ROW","CANARY_");stop(console);console=null;
        }
        startSearchConsole(certs.primary,"m32-client");
    }
    static Map<String,Object> typed(JdbcTemplate jdbc,UUID order,String type) {
        return jdbc.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=? AND event_type=?",order,type);
    }
    static Map<String,Object> wireEvidence(ConsumerRecord<String,String> record,UUID eventId) throws Exception {
        return Map.of("stage","broker-record","eventId",eventId,"topic",record.topic(),"partition",record.partition(),
                "offset",record.offset(),"key",record.key(),"rawSha256",java.util.HexFormat.of().formatHex(
                        java.security.MessageDigest.getInstance("SHA-256").digest(record.value().getBytes(StandardCharsets.UTF_8))));
    }
    static void assertLink(Map<String,Object> cause,Map<String,Object> effect) {
        assertThat(effect.get("correlation_id")).isEqualTo(cause.get("correlation_id"));
        assertThat(effect.get("causation_id")).isEqualTo(cause.get("event_id"));
    }
    static void paymentReady(Process process,Path log) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(()->{
            assertThat(process.isAlive()).as(read(log)).isTrue();
            assertThat(read(log)).contains("Started PaymentCellApplication","APEX reliable consumer active");
        });
    }
    static void assertNoDlt(Admin admin,List<String> sources) throws Exception {
        var request=new HashMap<TopicPartition,org.apache.kafka.clients.admin.OffsetSpec>();
        for(String source:sources) request.put(new TopicPartition(source+"-dlt",0),org.apache.kafka.clients.admin.OffsetSpec.latest());
        admin.listOffsets(request).all().get(10,TimeUnit.SECONDS).values().forEach(v->assertThat(v.offset()).isZero());
    }
}

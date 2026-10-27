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
class ThreeCellSagaIT {
    static final String REQUEST="apex.order.payment-requests.v1";
    static final String RELEASE="apex.order.inventory-release-requests.v1";
    static final String PAYMENT="apex.payment.events.v1";
    @Test void threeProcessesConfirmCompensateRejectDeduplicateAndRecoverQueuedPayment() throws Exception {
        Path root=Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        Path inventoryJar=Path.of(System.getProperty("saga.inventoryJar")).toAbsolutePath().normalize();
        Path orderJar=verifyBuild(root,inventoryJar),paymentJar=root.resolve("payment-cell/target/payment-cell.jar");
        var receipt=new Properties();
        try(var in=Files.newInputStream(root.resolve("out/m27-payment-build.properties"))) { receipt.load(in); }
        assertThat(sha(paymentJar)).isEqualTo(receipt.getProperty("paymentJarSha256"));
        try(var jar=new JarFile(paymentJar.toFile())) {
            assertThat(jar.getManifest().getMainAttributes().getValue("Start-Class")).isEqualTo("com.apex.payment.PaymentCellApplication");
        }
        Path output=inventoryJar.getParent(); var evidence=new ArrayList<Map<String,Object>>();
        var streams=List.of(ORDER_TOPIC,INVENTORY_TOPIC,REQUEST,RELEASE,PAYMENT,"apex.order.legacy.events.v1");
        var topics=new ArrayList<String>(); for(String s:streams) {topics.add(s);topics.add(s+"-dlt");}
        var broker=new EmbeddedKafkaKraftBroker(1,1,topics.toArray(String[]::new));
        broker.brokerProperty("auto.create.topics.enable","false");broker.brokerListProperty("apex.m27.test.brokers");
        Process order=null,inventory=null,payment=null;
        try(var orderDb=EmbeddedPostgres.builder().start();var invDb=EmbeddedPostgres.builder().start();
            var payDb=EmbeddedPostgres.builder().start();var http=HttpClient.newHttpClient()) {
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
                evidence.add(Map.of("stage","started","orderPid",order.pid(),"inventoryPid",inventory.pid(),"paymentPid",payment.pid(),
                        "orderGroup",og,"inventoryGroup",ig,"paymentGroup",pg,"brokers",brokers,"topics",topics));
                assertThat(new HashSet<>(List.of(order.pid(),inventory.pid(),payment.pid(),ProcessHandle.current().pid()))).hasSize(4);
                try(var admin=Admin.create(Map.of("bootstrap.servers",brokers));var wire=observer(brokers);var replay=producer(brokers)) {
                    wire.assign(streams.stream().map(s->new TopicPartition(s,0)).toList());wire.seekToBeginning(wire.assignment());
                    var cache=new HashMap<UUID,ConsumerRecord<String,String>>();
                    UUID a=UUID.randomUUID(),b=UUID.randomUUID();
                    stock.update("INSERT INTO inventory_stock VALUES (?,10,0),(?,10,0)",a,b);
                    long firstPaymentPid=payment.pid();stop(payment);payment=null;
                    UUID happy=UUID.fromString(post(http,op,List.of(item(a,2))).path("orderId").asText());
                    awaitStatus(orders,happy,"PENDING_PAYMENT");
                    assertThat(count(pay,"payment_authorizations","order_id",happy)).isZero();
                    pay.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')",happy);
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
                    stock.update("UPDATE release_failure_gate SET enabled=false");awaitStatus(orders,declined,"CANCELLED");
                    assertThat(getStatus(http,op,declined)).isEqualTo("CANCELLED");assertStock(stock,b,10,0);
                    assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,declined)).isEqualTo("RELEASED");
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
                        evidence.add(Map.of("stage","contradictory-payment-dlt","eventId",contradictoryId,
                                "sourceOffset",sentContradiction.offset(),"dltOffset",quarantined.offset(),
                                "dltTopic",quarantined.topic(),"orderStatus","CONFIRMED","inboxRows",0));
                    }
                }
                Files.writeString(output.resolve("three-cell-evidence.json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
            } finally {try{stop(payment);}finally{try{stop(inventory);}finally{try{stop(order);}finally{broker.destroy();}}}}
        }
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

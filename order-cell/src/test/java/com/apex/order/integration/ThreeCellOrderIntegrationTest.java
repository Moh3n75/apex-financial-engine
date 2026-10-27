package com.apex.order.integration;
import java.util.*;
import java.time.Instant;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import com.apex.order.support.DisposableOrderRuntime;
import com.apex.platform.events.*;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

@Tag("integration")
class ThreeCellOrderIntegrationTest {
    private static DisposableOrderRuntime runtime;
    private static JdbcTemplate jdbc;
    private static final JsonMapper JSON=JsonMapper.builder().build();
    @BeforeAll static void start() throws Exception {
        runtime=DisposableOrderRuntime.start(true,"--spring.profiles.active=three-cell-saga",
                "--apex.messaging.outbox.publisher.enabled=false");
        jdbc=runtime.application().getBean(JdbcTemplate.class);
    }
    @AfterAll static void close() throws Exception { if(runtime!=null) runtime.close(); }
    UUID order() {
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO orders(id,external_reference,description,status,created_at) VALUES (?,?,?,'PENDING_INVENTORY',now())",id,id.toString(),"test");
        return id;
    }
    void event(UUID orderId,String type) {
        String aggregate=type.startsWith("PAYMENT")?"PAYMENT_AUTHORIZATION":"INVENTORY_RESERVATION";
        var e=new EventEnvelope<>(new EventMetadata(UUID.randomUUID(),type,1,UUID.randomUUID(),null,orderId.toString(),aggregate,"test","test",Instant.now()),Map.of("orderId",orderId));
        runtime.application().getBean(ConfigurableKafkaEventConsumer.class).consume(new ConsumerRecord<>("outcomes",0,0,"key",JSON.writeValueAsString(e)),()->{});
    }
    long events(UUID id,String type) { return jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type=?",Long.class,id,type); }
    @Test void successAndLateBusinessDuplicatesEmitOneRequest() {
        UUID id=order(); event(id,"INVENTORY_RESERVED"); event(id,"INVENTORY_RESERVED");
        assertThat(events(id,"ORDER_READY_FOR_PAYMENT")).isEqualTo(1);
        event(id,"PAYMENT_AUTHORIZED"); event(id,"PAYMENT_AUTHORIZED"); event(id,"INVENTORY_RESERVED");
        assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?",String.class,id)).isEqualTo("CONFIRMED");
        assertThatThrownBy(()->event(id,"PAYMENT_DECLINED")).isInstanceOf(RuntimeException.class);
        assertThat(events(id,"ORDER_CANCELLATION_REQUESTED")).isZero();
    }
    @Test void declinedOrderWaitsForReleaseAndDoesNotSpawnDuplicateCompensation() {
        UUID id=order(); event(id,"INVENTORY_RESERVED"); event(id,"PAYMENT_DECLINED"); event(id,"PAYMENT_DECLINED");
        assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?",String.class,id)).isEqualTo("CANCELLING");
        assertThat(events(id,"ORDER_CANCELLATION_REQUESTED")).isEqualTo(1);
        event(id,"INVENTORY_RELEASED"); event(id,"INVENTORY_RELEASED"); event(id,"PAYMENT_DECLINED"); event(id,"INVENTORY_RESERVED");
        assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?",String.class,id)).isEqualTo("CANCELLED");
        assertThat(events(id,"ORDER_READY_FOR_PAYMENT")).isEqualTo(1);
    }
    @Test void stockRejectionCannotBeConfusedWithReleasedCancellation() {
        UUID id=order(); event(id,"INVENTORY_RESERVATION_REJECTED"); event(id,"INVENTORY_RESERVATION_REJECTED");
        assertThat(events(id,"ORDER_READY_FOR_PAYMENT")).isZero();
        assertThatThrownBy(()->event(id,"INVENTORY_RELEASED")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(()->event(id,"INVENTORY_RESERVED")).isInstanceOf(RuntimeException.class);
    }
    @Test void earlyOrMissingOutcomesRollbackInbox() {
        long before=jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class);
        UUID id=order();
        for(String type:List.of("PAYMENT_AUTHORIZED","PAYMENT_DECLINED","INVENTORY_RELEASED")) {
            assertThatThrownBy(()->event(id,type)).isInstanceOf(RuntimeException.class);
        }
        assertThatThrownBy(()->event(UUID.randomUUID(),"INVENTORY_RESERVED")).isInstanceOf(RuntimeException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class)).isEqualTo(before);
    }
    @Test void failedOutboxAppendRollsBackTransitionAndInbox() {
        long before=jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class);
        UUID id=order();
        jdbc.execute("CREATE FUNCTION fail_saga_append() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test append failure'; END $$");
        jdbc.execute("CREATE TRIGGER fail_saga_append BEFORE INSERT ON outbox_event FOR EACH ROW EXECUTE FUNCTION fail_saga_append()");
        try {
            assertThatThrownBy(()->event(id,"INVENTORY_RESERVED")).isInstanceOf(RuntimeException.class);
            assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?",String.class,id)).isEqualTo("PENDING_INVENTORY");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class)).isEqualTo(before);
        } finally { jdbc.execute("DROP TRIGGER fail_saga_append ON outbox_event"); jdbc.execute("DROP FUNCTION fail_saga_append()"); }
        event(id,"INVENTORY_RESERVED"); assertThat(events(id,"ORDER_READY_FOR_PAYMENT")).isEqualTo(1);
    }
}

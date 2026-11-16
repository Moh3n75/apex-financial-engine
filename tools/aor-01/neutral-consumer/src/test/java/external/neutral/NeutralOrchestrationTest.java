package external.neutral;

import com.apex.platform.messaging.jdbc.ConfigurableOutboxPublisher;
import com.apex.platform.messaging.jdbc.JacksonOutboxEventMapper;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Installed orchestration runs through owner ports with neither database driver on the classpath. */
class NeutralOrchestrationTest {
    @Test void serializesAndPublishesThroughOwnerStorageThenUsesItsFence() {
        var store=mock(OutboxStore.class);var transport=mock(KafkaMessagePublisher.class);
        var id=UUID.randomUUID();var fence=UUID.randomUUID();var aggregate=UUID.randomUUID();
        var record=new OutboxRecord(7L,id,"NEUTRAL_CHANGED",1,UUID.randomUUID(),null,aggregate.toString(),
                "PROBE","external","external",Instant.now(),"{\"amount\":1}",0,null,fence,Instant.now().plusSeconds(60),"owner");
        when(store.claimBatch(100,"owner",Duration.ofMinutes(5))).thenReturn(List.of(record));
        when(transport.publish(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(store.markPublished(7L,fence)).thenReturn(true);
        var mapper=JsonMapper.builder().build();
        new ConfigurableOutboxPublisher(store,transport,mapper,new JacksonOutboxEventMapper(mapper),new RetryPolicy(),"owner","neutral.events").publishBatch();
        var payload=ArgumentCaptor.forClass(String.class);
        verify(transport).publish(eq("neutral.events"),eq("PROBE:"+aggregate),payload.capture());
        assertThat(payload.getValue()).contains(id.toString(),"NEUTRAL_CHANGED","\"amount\":1");
        verify(store).markPublished(7L,fence);
        verify(store,never()).markFailed(any(),any(),any(),any());
    }
}

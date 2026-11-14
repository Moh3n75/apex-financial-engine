package com.apex.platform.grpc.internal;

import static org.assertj.core.api.Assertions.*;
import io.micrometer.observation.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class RpcObservationTest {
  @Test void originalFutureCancellationAndParentCollectionOutcomeArePreserved(){
    var outcomes=new ArrayList<String>();var registry=ObservationRegistry.create();
    registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>(){
      public boolean supportsContext(Observation.Context c){return true;}
      public void onStop(Observation.Context c){outcomes.add(c.getName()+":"+c.getLowCardinalityKeyValue("apex.outcome").getValue());}
    });var observer=new RpcObservation(registry);var original=new CompletableFuture<String>();
    var returned=observer.watch("collection",()->original);
    assertThat(returned).isSameAs(original);returned.toCompletableFuture().cancel(true);
    assertThat(original.isCancelled()).isTrue();assertThat(outcomes).containsExactly("apex.collection:CANCELLED");
  }
  @Test void failingObserverDoesNotChangeGatewayResult(){
    var registry=ObservationRegistry.create();registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>(){
      public boolean supportsContext(Observation.Context c){return true;}
      public void onStart(Observation.Context c){throw new IllegalStateException("CANARY");}
    });var original=CompletableFuture.completedFuture("reply");
    assertThat(new RpcObservation(registry).watch("query",()->original)).isSameAs(original);
  }
}

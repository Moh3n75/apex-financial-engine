package com.apex.platform.grpc.internal;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.apex.platform.grpc.api.GatewayException;
import io.grpc.ManagedChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.grpc.client.GrpcChannelFactory;

class CellGrpcChannelProviderTest {
  @Test void lazyConcurrentDiscoveryReusesChannelsAndClosesAllWithoutCreatingAfterClose() throws Exception {
    var count = new AtomicInteger();
    var channel = mock(ManagedChannel.class);
    when(channel.awaitTermination(anyLong(), any())).thenReturn(true);
    var p = new GrpcProperties(); p.setCellId("client-cell"); p.getClient().setEnabled(true);
    p.validate(true);
    var provider = new CellGrpcChannelProvider(p, mock(GrpcChannelFactory.class), id -> {count.incrementAndGet(); return channel;});
    assertThat(count).hasValue(0);
    try (var threads = Executors.newFixedThreadPool(8)) {
      var futures = new ArrayList<Future<ManagedChannel>>();
      for (int i=0;i<64;i++) futures.add(threads.submit(() -> provider.channel("inventory-cell")));
      for (var future : futures) assertThat(future.get(2, TimeUnit.SECONDS)).isSameAs(channel);
    }
    assertThat(count).hasValue(1);
    // There is no artificial 32-logical-destination ceiling in discovery mode.
    for (int i=0;i<40;i++) provider.channel("cell-"+i);
    assertThat(count).hasValue(41);
    provider.close(); provider.close();
    assertThatThrownBy(() -> provider.channel("payment-cell")).isInstanceOfSatisfying(GatewayException.class,
        e -> assertThat(e.code()).isEqualTo(GatewayException.Code.FAILED_PRECONDITION));
    assertThat(count).hasValue(41);
    verify(channel, atLeastOnce()).shutdown();
    verify(channel, never()).shutdownNow();
  }

  @Test void explicitAndLocalRoutesRetainTheirSemantics() throws Exception {
    var p = new GrpcProperties(); p.setCellId("client-cell");
    p.setDestinations(Map.of("local",new GrpcProperties.Destination(true,null),
        "one",new GrpcProperties.Destination(false,"shared"),"two",new GrpcProperties.Destination(false,"shared")));
    var factory=mock(GrpcChannelFactory.class); var channel=mock(ManagedChannel.class);
    when(factory.createChannel("shared")).thenReturn(channel);
    when(channel.awaitTermination(anyLong(),any())).thenReturn(true);
    try(var provider=new CellGrpcChannelProvider(p,factory,null)) {
      verifyNoInteractions(factory);
      assertThat(provider.channel("one")).isSameAs(provider.channel("two"));
      verify(factory,times(1)).createChannel("shared");
      assertThatThrownBy(()->provider.channel("missing")).isInstanceOfSatisfying(GatewayException.class,
          e->assertThat(e.code()).isEqualTo(GatewayException.Code.NOT_FOUND));
      assertThatThrownBy(()->provider.channel("local")).isInstanceOfSatisfying(GatewayException.class,
          e->assertThat(e.code()).isEqualTo(GatewayException.Code.INVALID_ARGUMENT));
    }
    verify(channel).shutdown();
  }
}

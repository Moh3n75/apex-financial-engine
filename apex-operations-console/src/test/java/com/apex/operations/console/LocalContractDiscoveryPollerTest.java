package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.apex.operations.contracts.*;
import com.netflix.discovery.*;
import com.netflix.discovery.shared.*;
import com.netflix.appinfo.InstanceInfo;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.env.MockEnvironment;

/** Synthetic registry + actual HTTP exchange. Not a live Cell receipt. */
class LocalContractDiscoveryPollerTest {
  ContractManifest manifest(String environment,String owner,String cluster,String mode,Instant collected){return new ContractManifest(1,environment,owner,owner,"test",UUID.randomUUID(),collected,mode,cluster,List.of(),List.of(),"",List.of(),List.of(),List.of());}
  MockEnvironment env(){return new MockEnvironment().withProperty("apex.ops.local-inventory.environment","local").withProperty("apex.ops.local-contracts.cluster","cluster-a");}
  LocalContractDiscoveryPoller poller(DefaultListableBeanFactory beans){return new LocalContractDiscoveryPoller(beans.getBeanProvider(EurekaClient.class),beans.getBeanProvider(EurekaClientConfig.class),env());}
  @Test void realHttpReadsAreEnumeratedOnlyFromAdvertisedLocalRegistryEntriesAndDisappearanceReplacesCache() throws Exception {
    var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);var payload=manifest("local","sample-cell","cluster-a","EFFECTIVE_DECLARATION",Instant.now());
    byte[] bytes=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules().writeValueAsBytes(payload);
    server.createContext("/ops/v1/contracts",exchange->{exchange.sendResponseHeaders(200,bytes.length);try(var output=exchange.getResponseBody()){output.write(bytes);}});server.start();
    var beans=new DefaultListableBeanFactory();var client=mock(DiscoveryClient.class);var config=mock(EurekaClientConfig.class);
    beans.registerSingleton("client",client);beans.registerSingleton("config",config);
    when(config.getEurekaServerServiceUrls("defaultZone")).thenReturn(List.of("http://127.0.0.1:8761/eureka/"));
    when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(0L);
    var apps=new Applications();var app=new Application("SAMPLE-CELL");
    app.addInstance(InstanceInfo.Builder.newBuilder().setAppName("SAMPLE-CELL").setInstanceId("private:host:port").setHostName("private")
        .setIPAddr("127.0.0.1").setPort(server.getAddress().getPort()).setStatus(InstanceInfo.InstanceStatus.UP)
        .setMetadata(Map.of("apex-contract-discovery-http","true")).build());apps.addApplication(app);when(client.getApplications()).thenReturn(apps);
    try(var provider=poller(beans)){
      provider.poll();assertThat(provider.views()).singleElement().satisfies(v->{assertThat(v.manifest()).isEqualTo(payload);assertThat(v.transport()).isEqualTo("LOCAL_HTTP");});
      assertThat(provider.coverage().status()).isEqualTo("AVAILABLE");
      assertThat(provider.views().toString()).doesNotContain("private:host:port");
      when(client.getApplications()).thenReturn(new Applications());provider.poll();assertThat(provider.views()).isEmpty();assertThat(provider.coverage().status()).isEqualTo("NOT_CONFIGURED");
      when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(Long.MAX_VALUE);provider.poll();assertThat(provider.coverage().status()).isEqualTo("SOURCE_UNAVAILABLE");
    }finally{server.stop(0);}
  }
  @Test void scopeClusterSourceAndExpiryAreCheckedWithoutStaticFallback() {
    try(var provider=poller(new DefaultListableBeanFactory())){
      for(var candidate:List.of(manifest("prod","sample-cell","cluster-a","EFFECTIVE_DECLARATION",Instant.now()),
          manifest("local","other-cell","cluster-a","EFFECTIVE_DECLARATION",Instant.now()),
          manifest("local","sample-cell","wrong-cluster","EFFECTIVE_DECLARATION",Instant.now()),
          manifest("local","sample-cell","cluster-a","STATIC_DECLARATION",Instant.now()),
          manifest("local","sample-cell","cluster-a","EFFECTIVE_DECLARATION",Instant.now().minusSeconds(40))))
        assertThatThrownBy(()->provider.validate(candidate,"sample-cell",Instant.now())).hasMessage("MANIFEST_SCOPE_OR_FRESHNESS");
    }
  }
}

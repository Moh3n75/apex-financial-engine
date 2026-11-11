package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.TopologySnapshot;
import com.apex.operations.contracts.TopologySnapshot.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;

/** HTTP integration against an explicitly synthetic Actuator response; not a live Gateway receipt. */
class GatewayRouteTopologyTest {
  HttpServer server;
  java.util.concurrent.ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor();
  String body="[]"; int status=200, calls=0; boolean slow=false;
  String definitions="""
    [{"id":"actual_order_route","uri":"lb://ORDER-CELL","predicates":[{"name":"Path","args":{"pattern":"/order-cell/**"}}],"filters":[{"name":"RewritePath","args":{"secret":"never-export"}}]},
     {"id":"self","uri":"lb://APEX-API-GATEWAY","predicates":[{"name":"Path","args":{"pattern":"/apex-api-gateway/**"}}],"filters":[]}]
    """;
  @BeforeEach void start() throws Exception {
    server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.setExecutor(executor);
    server.createContext("/actuator/gateway/routes", exchange->{
      calls++; assertThat(exchange.getRequestMethod()).isEqualTo("GET");
      byte[] bytes=body.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(status,slow?0:bytes.length);
      try(var output=exchange.getResponseBody()){
        if(slow){output.write('[');output.flush();Thread.sleep(3000);}else output.write(bytes);
      }catch(InterruptedException interrupted){Thread.currentThread().interrupt();}catch(java.io.IOException cancelled){}
    });
    server.createContext("/actuator/gateway/routedefinitions",exchange->{byte[] bytes=definitions.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);try(var output=exchange.getResponseBody()){output.write(bytes);}});
    server.start();
  }
  @AfterEach void stop(){server.stop(0);executor.shutdownNow();}
  MockEnvironment env(){return new MockEnvironment().withProperty("apex.ops.local-inventory.environment","local")
      .withProperty("apex.ops.gateway-routes.origin","http://127.0.0.1:"+server.getAddress().getPort());}
  TopologySnapshot base(){
    var now=Instant.now();var proof=new Evidence("OBSERVED","EUREKA","UP_REGISTRATION_LOCAL_REGISTRY",now,now,"SERVICE");
    var nodes=List.of("apex-api-gateway","order-cell").stream().map(s->new Node("local:service:"+s,"SERVICE",s,"local","local:service:"+s,
        null,null,"SERVICE","UNKNOWN","FRESH","REGISTRY_PRESENCE",now,List.of(proof))).toList();
    return new TopologySnapshot(2,now,"TEST_FIXTURE",new Capabilities("UNKNOWN","UNKNOWN","NOT_CONFIGURED","NOT_CONFIGURED","NOT_CONFIGURED","NOT_CONFIGURED"),nodes,List.of(),List.of(),List.of(),List.of());
  }
  @Test void onlyRegisteredLogicalRoutesBecomeDeclaredEdgesAndReadsAreCached() {
    body="""
      [{"route_id":"actual_order_route","uri":"lb://ORDER-CELL","filters":["private-secret"]},
       {"route_id":"self","uri":"lb://APEX-API-GATEWAY"}]
      """;
    try(var provider=new GatewayRouteTopology(env())){
      var graph=provider.join(base(),(e,o)->true);
      assertThat(graph.connections()).hasSize(2).allSatisfy(edge->{
        assertThat(edge.protocol()).isEqualTo("HTTP");assertThat(edge.health()).isEqualTo("UNKNOWN");assertThat(edge.lastSeen()).isNull();
        assertThat(edge.evidence()).allSatisfy(p->{assertThat(p.kind()).isEqualTo("DECLARED");assertThat(p.source()).isEqualTo("SPRING_CLOUD_GATEWAY");});
      });
      assertThat(graph.toString()).doesNotContain("private-secret", "127.0.0.1", "lb://");
      assertThat(graph.connections().getFirst().details().http().predicates()).containsExactly("Path: /order-cell/**");
      assertThat(graph.toString()).doesNotContain("never-export");
      assertThat(graph.providers().getLast().status()).isEqualTo("AVAILABLE");
      provider.join(base(),(e,o)->true);assertThat(calls).isEqualTo(1);
    }
  }
  @Test void emptyDefinitionsNeverCreateEdgesFromRegistryPresence() {
    try(var provider=new GatewayRouteTopology(env())){
      var graph=provider.join(base(),(e,o)->true);assertThat(graph.connections()).isEmpty();
      assertThat(graph.nodes()).hasSize(2);assertThat(graph.providers().getLast().status()).isEqualTo("AVAILABLE");
    }
  }
  @Test void unknownPhysicalMalformedAndDuplicateRoutesStayUnresolved() {
    body="""
      [{"route_id":"unknown","uri":"lb://MISSING-CELL"},{"route_id":"physical","uri":"http://user:secret@private-host:8080"},
       {"route_id":"duplicate","uri":"lb://ORDER-CELL"},{"route_id":"duplicate","uri":"lb://ORDER-CELL"},
       {"route_id":"bad","uri":":::"}]
      """;
    try(var provider=new GatewayRouteTopology(env())){
      var graph=provider.join(base(),(e,o)->true);assertThat(graph.connections()).isEmpty();
      assertThat(graph.providers().getLast().status()).isEqualTo("PARTIAL");
      assertThat(graph.unresolved()).extracting(Unresolved::reason).contains("ROUTE_TARGET_NOT_DISCOVERED","NON_LOGICAL_ROUTE_DESTINATION","DUPLICATE_ROUTE_ID","INVALID_ROUTE_DESTINATION");
      assertThat(graph.toString()).doesNotContain("private-host","secret");
    }
  }
  @Test void deniedGatewayIsNotPolledAndDeniedTargetsAreRemovedBeforeAggregation() {
    body="""
      [{"route_id":"hidden_order","uri":"lb://ORDER-CELL"}]
      """;
    try(var provider=new GatewayRouteTopology(env())){
      assertThat(provider.join(base(),(e,o)->false).providers().getLast().status()).isEqualTo("NO_VISIBLE_COVERAGE");
      assertThat(calls).isZero();
      var graph=provider.join(base(),(e,o)->o.equals("apex-api-gateway"));
      assertThat(graph.connections()).isEmpty();assertThat(graph.unresolved()).isEmpty();
      assertThat(graph.toString()).doesNotContain("hidden_order");
    }
  }
  @Test void deniedUnavailableAndInvalidResponsesRemainTruthful() {
    for(int code:List.of(403,503)){
      status=code;
      try(var provider=new GatewayRouteTopology(env())){
        var graph=provider.join(base(),(e,o)->true);
        assertThat(graph.connections()).isEmpty();
        assertThat(graph.providers().getLast().status()).isEqualTo(code==403?"PERMISSION_GAP":"SOURCE_UNAVAILABLE");
      }
    }
    status=200;body="{}";
    try(var provider=new GatewayRouteTopology(env())){assertThat(provider.join(base(),(e,o)->true).providers().getLast().status()).isEqualTo("SOURCE_INVALID");}
  }
  @Test void bodySizeAndSlowStreamingAreBoundedAcrossTheWholeExchange() {
    body="["+" ".repeat(70000);
    try(var provider=new GatewayRouteTopology(env())){assertThat(provider.join(base(),(e,o)->true).connections()).isEmpty();}
    slow=true;
    try(var provider=new GatewayRouteTopology(env())){
      long start=System.nanoTime();
      assertThat(provider.join(base(),(e,o)->true).providers().getLast().status()).isEqualTo("SOURCE_UNAVAILABLE");
      assertThat(java.time.Duration.ofNanos(System.nanoTime()-start).toMillis()).isLessThan(2800);
    }
  }
  @Test void originsAreLocalOnlyAndExistingSourcesArePreserved() {
    assertThatThrownBy(()->new GatewayRouteTopology(env().withProperty("apex.ops.gateway-routes.origin","http://remote.internal:8080")))
        .hasMessage("LOCAL_GATEWAY_ORIGIN_REQUIRED");
    assertThatThrownBy(()->new GatewayRouteTopology(env().withProperty("apex.ops.local-inventory.environment","prod")))
        .hasMessage("LOCAL_GATEWAY_ORIGIN_REQUIRED");
    var base=base();
    var proof=new Evidence("DECLARED","CONTRACT_DISCOVERY","TEST_CONTRACT",Instant.now(),null,"SERVICE");
    var existing=new Connection("existing","local:service:apex-api-gateway","local:service:order-cell","local","GRPC","SERVICE","EXPLICIT_QUERY:1","UNKNOWN",null,List.of(proof));
    base=new TopologySnapshot(2,base.asOf(),base.coverage(),base.capabilities(),base.nodes(),List.of(existing),List.of(new Provider("contracts","PARTIAL","TEST_FIXTURE","STATIC_DECLARATION",null)),List.of(),List.of());
    try(var provider=new GatewayRouteTopology(env())){
      var result=provider.join(base,(e,o)->true);assertThat(result.connections()).containsExactly(existing);
      assertThat(result.providers().getFirst()).isEqualTo(base.providers().getFirst());
    }
  }
}

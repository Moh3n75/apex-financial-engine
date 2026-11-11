package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.*;
import com.apex.operations.contracts.ContractManifest.*;
import com.apex.operations.contracts.TopologySnapshot.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Explicit contract fixtures, never claimed as live calls. */
class CommunicationTopologyTest {
  ContractManifest manifest(String service,List<Publication> publications,List<String> topics,String group,List<Contract> events,List<Rpc> handlers,List<GrpcDependency> calls) {
    return new ContractManifest(1,"local",service,service,"test",UUID.randomUUID(),Instant.now(),"FIXTURE","cluster-a",publications,topics,group,events,handlers,List.of(),calls);
  }
  TopologySnapshot graph(ContractManifest... manifests){return ContractTopology.join(TopologyApi.project(List.of(),Instant.now(),(e,o)->true),
      Arrays.stream(manifests).map(m->new ContractDiscoveryPoller.View(m.environment(),m.service(),m,"AVAILABLE")).toList(),(e,o)->true);}
  @Test void multipleEventsOnOneTopicPreserveEachPublisherConsumerAndMatchingHandler() {
    var a=new Contract("EVENT_A",1);var b=new Contract("EVENT_B",2);
    var publisher=manifest("producer",List.of(new Publication(a,"shared.events","PUBLISHER_ROUTING"),new Publication(b,"shared.events","PUBLISHER_ROUTING")),List.of(),"",List.of(),List.of(),List.of());
    var first=manifest("consumer-one",List.of(),List.of("shared.events"),"group-one",List.of(a),List.of(),List.of());
    var second=manifest("consumer-two",List.of(),List.of("shared.events"),"group-two",List.of(b),List.of(),List.of());
    var graph=graph(publisher,first,second);
    assertThat(graph.connections()).hasSize(6);
    assertThat(graph.connections()).allSatisfy(edge->{
      assertThat(edge.protocol()).isEqualTo("KAFKA");assertThat(edge.lastSeen()).isNull();
      assertThat(edge.details().kafka()).singleElement().satisfies(match->{
        assertThat(match.matchStatus()).isEqualTo("EXACT_DECLARED_CONTRACT_MATCH");
        assertThat(match.handlerContract()).isEqualTo(edge.contract());
        assertThat(match.consumerServiceId()).endsWith(edge.contract().equals("EVENT_A:1")?"consumer-one":"consumer-two");
      });
    });
  }
  @Test void matchingQueriesAndRequestsRequireCallerDeclarationAndTargetHandler() {
    var q=new Contract("READ_SAMPLE",1);var r=new Contract("WRITE_SAMPLE",2);
    var target=manifest("target",List.of(),List.of(),"",List.of(),List.of(new Rpc("QUERY",q),new Rpc("REQUEST",r)),List.of());
    var caller=manifest("caller",List.of(),List.of(),"",List.of(),List.of(),List.of(
        new GrpcDependency("QUERY",q,"target","target","REQUEST_RESPONSE","DISCOVERY_CONFIGURED"),
        new GrpcDependency("REQUEST",r,"target","target","REQUEST_RESPONSE","CONFIGURED")));
    var graph=graph(caller,target);
    assertThat(graph.connections()).hasSize(2).allSatisfy(e->{assertThat(e.protocol()).isEqualTo("GRPC");assertThat(e.lastSeen()).isNull();assertThat(e.details().grpc().callerServiceId()).endsWith("caller");});
    assertThat(graph.connections()).extracting(e->e.details().grpc().kind()).containsExactly("QUERY","REQUEST");
    assertThat(graph(target).connections()).isEmpty();
    assertThat(graph(target).unresolved()).allSatisfy(u->assertThat(u.reason()).isEqualTo("NO_VISIBLE_GRPC_CALLER_DECLARATION"));
    var missing=manifest("target",List.of(),List.of(),"",List.of(),List.of(new Rpc("QUERY",new Contract("READ_SAMPLE",99))),List.of());
    assertThat(graph(caller,missing).connections()).isEmpty();
    assertThat(graph(caller,missing).unresolved()).extracting(Unresolved::reason).contains("NO_MATCHING_TARGET_GRPC_HANDLER");
  }
}

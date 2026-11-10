package com.apex.operations.console;
import static org.assertj.core.api.Assertions.*;
import com.apex.platform.discovery.*;
import com.apex.platform.grpc.api.*;
import com.apex.operations.contracts.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class RuntimeMembershipPollerTest {
  final List<CellMembership.Member> entries=new ArrayList<>();
  boolean complete=true,offline=false;
  final Set<String> failed=new HashSet<>();
  final CellServiceDirectory directory=new CellServiceDirectory(){
    public List<CellEndpoint>endpoints(String cell){return entries.stream().map(CellMembership.Member::endpoint).toList();}
    public CellMembership membership(String cell,String environment){if(offline)throw new IllegalStateException();return new CellMembership("EUREKA","test-registry",complete?"SCOPED_REGISTRY_COMPLETE":"PARTIAL",Instant.now(),entries);}
  };
  final InstanceQueryGateway gateway=new InstanceQueryGateway(){public<R> R queryInstance(String cell,String id,Object query,Class<R> type,QueryOptions options){
    if(failed.contains(id))throw new GatewayException(GatewayException.Code.UNAVAILABLE,false);
    var e=entries.stream().filter(m->m.registryId().equals(id)).findFirst().orElseThrow();
    return type.cast(new ContractManifest(1,"test",cell,cell,e.instanceId(),e.epoch(),Instant.now(),"EFFECTIVE_DECLARATION","test",List.of(),List.of(),"",List.of(),List.of(),List.of()));
  }};
  RuntimeMembershipPoller poller(){return new RuntimeMembershipPoller(new MockEnvironment().withProperty("apex.ops.contract-discovery.environment","test")
      .withProperty("apex.ops.contract-discovery.owners","order-cell").withProperty("eureka.client.serviceUrl.defaultZone","https://registry/eureka/")
      .withProperty("eureka.client.tls.enabled","true"),directory,gateway);}
  CellMembership.Member member(String registry,String logical,int port){return new CellMembership.Member("test","order-cell",logical,UUID.randomUUID(),registry,new CellEndpoint("order-cell",registry,"127.0.0.1",port,port+1,false,"localhost"));}
  List<String> states(RuntimeMembershipPoller p){return p.views().getFirst().samples().stream().map(s->s.member().membershipState()).toList();}
  @Test void overlapDetectsDuplicatesButSequentialRestartAndRollingReplicasDoNot(){try(var p=poller()){
    entries.add(member("first","one",8000));p.scan("order-cell",Instant.now());assertThat(states(p)).containsExactly("VERIFIED");
    entries.add(member("second","two",8002));p.scan("order-cell",Instant.now());assertThat(states(p)).doesNotContain("DUPLICATE");
    entries.remove(1);entries.add(member("second-new","two",8004));p.scan("order-cell",Instant.now());assertThat(states(p)).doesNotContain("DUPLICATE");
    entries.add(member("conflict","one",8006));p.scan("order-cell",Instant.now());assertThat(states(p).stream().filter(s->s.equals("DUPLICATE"))).hasSize(2);
    failed.add("conflict");p.scan("order-cell",Instant.now());assertThat(states(p)).doesNotContain("DUPLICATE");
    complete=false;failed.clear();p.scan("order-cell",Instant.now());assertThat(states(p)).doesNotContain("DUPLICATE");
    complete=true;entries.clear();p.scan("order-cell",Instant.now());assertThat(states(p)).containsOnly("DISAPPEARED");
    offline=true;p.scan("order-cell",Instant.now());assertThat(states(p)).containsOnly("DISAPPEARED");
    var gone=RuntimeTopology.project(TopologyApi.project(List.of(),Instant.now(),(e,o)->true),p.views(),"test",(e,o)->true);
    assertThat(gone.descriptors()).isEmpty();assertThat(gone.connections()).isEmpty();
  }}
  @Test void filteringPrecedesAggregationAndEndpointsNeverAppearOnWire(){try(var p=poller()){
    entries.add(member("one","one",9000));p.scan("order-cell",Instant.now());
    var base=TopologyApi.project(List.of(),Instant.now(),(e,o)->true);
    var hidden=RuntimeTopology.project(base,p.views(),"test",(e,o)->false);assertThat(hidden.nodes()).isEmpty();assertThat(hidden.memberships()).isEmpty();
    var visible=RuntimeTopology.project(base,p.views(),"test",(e,o)->true);
    assertThat(visible.memberships()).hasSize(1);
    assertThat(tools.jackson.databind.json.JsonMapper.builder().build().writeValueAsString(visible)).doesNotContain("127.0.0.1","9000","localhost","keystore");
  }}
}

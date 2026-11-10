package com.apex.operations.console;
import com.apex.operations.contracts.*;
import com.apex.operations.contracts.TopologySnapshot.*;
import java.util.*;
import java.util.function.BiPredicate;

final class RuntimeTopology {
  static TopologySnapshot project(TopologySnapshot base,List<RuntimeMembershipPoller.Owner> input,
      String environment,BiPredicate<String,String> visible) {
    var owners=input.stream().filter(o->visible.test(environment,o.service())).toList();
    var samples=owners.stream().flatMap(o->o.samples().stream()).filter(s->s.member().environment().equals(environment)).toList();
    var declarations=samples.stream().filter(s->s.manifest()!=null&&!s.member().membershipState().equals("DISAPPEARED"))
        .map(s->new ContractDiscoveryPoller.View(environment,s.member().service(),s.manifest(),s.member().transportStatus())).toList();
    var graph=ContractTopology.join(base,declarations,visible);
    var nodes=new LinkedHashMap<String,Node>();graph.nodes().forEach(n->nodes.put(n.id(),n));
    var providers=new ArrayList<>(graph.providers());
    var members=new ArrayList<RuntimeMember>();
    boolean truncated=false;
    for(var owner:owners) {
      providers.add(new Provider("membership:"+environment+":"+owner.service(),owner.status(),owner.coverage(),"REGISTRY_AND_MTLS",owner.collectedAt()));
      for(var sample:owner.samples()) {
        var row=sample.member();if(!row.environment().equals(environment))continue;
        if(nodes.size()>=190||members.size()>=128){truncated=true;continue;}
        String service=environment+":service:"+row.service();
        var evidence=new Evidence("OBSERVED",row.lastSeen()==null?"REGISTRY":"REGISTRY_AND_MTLS",row.discoverySource()+":"+row.registryIdentity(),owner.collectedAt(),row.lastSeen(),"INSTANCE");
        nodes.putIfAbsent(service,new Node(service,"SERVICE",row.service(),environment,service,null,null,"SERVICE","UNKNOWN",owner.status(),"PARTIAL_MEMBERSHIP",null,List.of(evidence)));
        nodes.put(row.id(),new Node(row.id(),"INSTANCE",row.instanceId(),environment,service,row.instanceId(),row.epoch(),"INSTANCE","UNKNOWN",row.freshness(),row.membershipState(),row.lastSeen(),List.of(evidence)));
        members.add(row);
      }
      String service=environment+":service:"+owner.service();
      var previous=nodes.get(service);
      if(previous!=null) {
        boolean duplicate=owner.samples().stream().anyMatch(s->s.member().membershipState().equals("DUPLICATE"));
        nodes.put(service,new Node(previous.id(),previous.kind(),previous.name(),previous.environment(),previous.serviceId(),null,null,"SERVICE","UNKNOWN",owner.status(),
            duplicate?"DUPLICATE_INSTANCE_ID":owner.coverage(),previous.lastSeen(),previous.evidence()));
      }
    }
    if(truncated)providers.add(new Provider("membership-budget","PARTIAL","MEMBERSHIP_NODE_LIMIT","REGISTRY_AND_MTLS",graph.asOf()));
    return new TopologySnapshot(2,graph.asOf(),"AUTHORIZED_SCOPED_RUNTIME_MEMBERSHIP_AND_DECLARATIONS",new Capabilities(graph.capabilities().declared(),"REGISTRY_AND_MTLS_SCOPED_MEMBERSHIP",graph.capabilities().observedInteractions(),graph.capabilities().messaging(),graph.capabilities().traces(),graph.capabilities().runtimeHealth()),
        List.copyOf(nodes.values()),graph.connections(),providers,graph.unresolved(),graph.descriptors(),members);
  }
}

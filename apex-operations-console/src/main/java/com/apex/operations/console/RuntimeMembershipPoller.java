package com.apex.operations.console;

import com.apex.operations.contracts.*;
import com.apex.operations.contracts.TopologySnapshot.RuntimeMember;
import com.apex.platform.discovery.*;
import com.apex.platform.grpc.api.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Uses authoritative directory snapshots and pinned mTLS reads; never creates a registry. */
@Component
@ConditionalOnProperty(name="apex.ops.membership.enabled",havingValue="true")
public final class RuntimeMembershipPoller implements AutoCloseable {
  public record Sample(RuntimeMember member,ContractManifest manifest) {}
  public record Owner(String service,String status,String coverage,Instant collectedAt,List<Sample> samples) {}
  private final CellServiceDirectory directory;
  private final InstanceQueryGateway gateway;
  private final String environment;
  private final int contractVersion;
  private final List<String> owners;
  private final Map<String,Owner> cache=new LinkedHashMap<>();
  private final ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("ops-membership").factory());
  public RuntimeMembershipPoller(Environment env,CellServiceDirectory directory,InstanceQueryGateway gateway) {
    this.directory=directory;this.gateway=gateway;environment=env.getRequiredProperty("apex.ops.contract-discovery.environment");
    contractVersion=env.getProperty("apex.ops.contract-discovery.contract-version",Integer.class,1);
    if(contractVersion!=1&&contractVersion!=2)throw new IllegalArgumentException("CONTRACT_VERSION");
    owners=List.of(env.getRequiredProperty("apex.ops.contract-discovery.owners").split(","));
    if(owners.size()>8||environment.equals("unknown"))throw new IllegalArgumentException("MEMBERSHIP_SCOPE");
    owners.forEach(CellEndpoint::logicalName);
    String provider=env.getProperty("apex.discovery.provider","eureka");
    if(provider.equals("eureka")&&(!env.getProperty("eureka.client.serviceUrl.defaultZone","").startsWith("https://")||!env.getProperty("eureka.client.tls.enabled",Boolean.class,false)))
      throw new IllegalArgumentException("MEMBERSHIP_REQUIRES_AUTHENTICATED_REGISTRY");
    if(!Set.of("eureka","kubernetes").contains(provider))throw new IllegalArgumentException("UNSUPPORTED_MEMBERSHIP_PROVIDER");
  }
  @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
  public void start(){timer.scheduleWithFixedDelay(this::poll,0,3,TimeUnit.SECONDS);}
  void poll(){for(String owner:owners)scan(owner,Instant.now());}

  void scan(String owner,Instant now) {
    try {
      var before=directory.membership(owner,environment);
      if(before.members().size()>16)throw new IllegalStateException("INSTANCE_BUDGET");
      var next=new ArrayList<Sample>();
      for(var entry:before.members()) {
        ContractManifest manifest=null;String state="UNVERIFIED",transport="SOURCE_UNAVAILABLE";
        try {
          manifest=gateway.queryInstance(owner,entry.registryId(),contractVersion==2?new ContractDiscoveryQueryV2():new ContractDiscoveryQuery(),ContractManifest.class,new QueryOptions(UUID.randomUUID(),Duration.ofMillis(1500)));
          if(!manifest.service().equals(owner)||!manifest.cell().equals(entry.cellId())||!manifest.environment().equals(environment)
              ||!manifest.instance().equals(entry.instanceId())||!manifest.epoch().equals(entry.epoch())
              ||!manifest.sourceMode().equals("EFFECTIVE_DECLARATION")||manifest.collectedAt().isBefore(now.minusSeconds(30))
              ||manifest.collectedAt().isAfter(Instant.now().plusSeconds(1)))throw new IllegalArgumentException("MEMBERSHIP_IDENTITY");
          state="VERIFIED";transport="AVAILABLE";
        }catch(GatewayException denied){transport=denied.code()==GatewayException.Code.PERMISSION_DENIED||denied.code()==GatewayException.Code.UNAUTHENTICATED?"PERMISSION_GAP":"SOURCE_UNAVAILABLE";manifest=null;}
        catch(RuntimeException rejected){state="REJECTED";transport="IDENTITY_REJECTED";manifest=null;}
        String id=environment+":instance:"+owner+":"+Base64.getUrlEncoder().withoutPadding().encodeToString(entry.registryId().getBytes(java.nio.charset.StandardCharsets.UTF_8))+":"+entry.epoch();
        Instant lastSeen=manifest==null?null:manifest.collectedAt();String freshness="FRESH";
        if(manifest==null&&transport.equals("SOURCE_UNAVAILABLE"))synchronized(cache) {
          var previous=cache.get(owner);
          if(previous!=null)for(var old:previous.samples())if(old.member().id().equals(id)) {
            lastSeen=old.member().lastSeen();manifest=old.manifest();freshness="STALE";
          }
        }
        next.add(new Sample(new RuntimeMember(id,environment,entry.cellId(),owner,entry.instanceId(),entry.epoch(),entry.registryId(),
            before.registryIdentity(),before.source(),lastSeen,freshness,state,before.coverage(),transport),manifest));
      }
      var after=directory.membership(owner,environment);
      var stillPresent=after.members().stream().map(m->m.registryId()+":"+m.epoch()).collect(java.util.stream.Collectors.toSet());
      var initial=before.members().stream().map(m->m.registryId()+":"+m.epoch()).collect(java.util.stream.Collectors.toSet());
      boolean overlap=before.authoritative()&&after.authoritative()&&before.registryIdentity().equals(after.registryIdentity())&&initial.equals(stillPresent);
      var verified=next.stream().filter(s->s.member().transportStatus().equals("AVAILABLE")&&s.member().lastSeen()!=null
          &&s.member().lastSeen().isAfter(Instant.now().minusSeconds(15))&&stillPresent.contains(s.member().registryId()+":"+s.member().epoch())).toList();
      var duplicates=new HashSet<String>();
      if(overlap)for(var sample:verified)if(verified.stream().filter(s->s.member().instanceId().equals(sample.member().instanceId()))
          .map(s->s.member().epoch()).distinct().count()>1)duplicates.add(sample.member().instanceId());
      var result=new ArrayList<Sample>();
      for(var sample:next) {
        var row=sample.member();String state=row.membershipState();
        if(!stillPresent.contains(row.registryId()+":"+row.epoch()))state=overlap?"DISAPPEARED":"UNKNOWN";
        else if(duplicates.contains(row.instanceId())&&row.transportStatus().equals("AVAILABLE"))state="DUPLICATE";
        result.add(new Sample(change(row,state,row.freshness(),row.transportStatus()),sample.manifest()));
      }
      synchronized(cache) {
        var previous=cache.get(owner);
        if(previous!=null)for(var old:previous.samples())if(result.stream().noneMatch(s->s.member().id().equals(old.member().id()))
            &&old.member().lastSeen()!=null&&old.member().lastSeen().isAfter(now.minusSeconds(120))&&result.size()<32)
          result.add(new Sample(change(old.member(),overlap||old.member().membershipState().equals("DISAPPEARED")?"DISAPPEARED":"UNKNOWN","STALE","NOT_ATTEMPTED"),old.manifest()));
        String coverage=overlap?"SCOPED_REGISTRY_COMPLETE":"PARTIAL_REGISTRY_COVERAGE";
        String status=overlap&&next.stream().allMatch(s->s.member().transportStatus().equals("AVAILABLE"))?"AVAILABLE":"PARTIAL";
        cache.put(owner,new Owner(owner,status,coverage,after.collectedAt(),List.copyOf(result)));
      }
    }catch(RuntimeException unavailable) {
      synchronized(cache) {
        var old=cache.get(owner);
        cache.put(owner,new Owner(owner,"REGISTRY_UNAVAILABLE","UNKNOWN_CURRENT_MEMBERSHIP",now,
            old==null?List.of():old.samples().stream().map(s->new Sample(change(s.member(),s.member().membershipState().equals("DISAPPEARED")?"DISAPPEARED":"UNKNOWN","STALE","REGISTRY_UNAVAILABLE"),s.manifest())).toList()));
      }
    }
  }
  static RuntimeMember change(RuntimeMember row,String state,String freshness,String transport) {
    return new RuntimeMember(row.id(),row.environment(),row.cell(),row.service(),row.instanceId(),row.epoch(),row.registryId(),row.registryIdentity(),row.discoverySource(),row.lastSeen(),freshness,state,row.coverage(),transport);
  }
  public List<Owner> views(){synchronized(cache){return cache.values().stream().map(o->o.collectedAt().isBefore(Instant.now().minusSeconds(15))
      ?new Owner(o.service(),"STALE","UNKNOWN_CURRENT_MEMBERSHIP",o.collectedAt(),o.samples().stream().map(s->new Sample(change(s.member(),s.member().membershipState().equals("DISAPPEARED")?"DISAPPEARED":"UNKNOWN","STALE",s.member().transportStatus()),s.manifest())).toList()):o).toList();}}
  @Override public void close(){timer.shutdownNow();}
}

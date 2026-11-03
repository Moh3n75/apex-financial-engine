package com.apex.operations.console;
import com.apex.operations.contracts.*;import com.apex.platform.grpc.api.*;
import java.time.*;import java.util.*;import java.util.concurrent.*;
import org.springframework.beans.factory.ObjectProvider;import org.springframework.core.env.Environment;import org.springframework.stereotype.Component;
@Component
public final class OwnerPoller implements AutoCloseable {
 private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("ops-owner-read").factory());
 private final Map<String,EvidenceCache> caches=new LinkedHashMap<>();private final List<UUID> events,workflows;private final QueryGateway gateway;
 private final Map<String,Integer> failures=new HashMap<>();private final Map<String,Instant> next=new HashMap<>();
 public OwnerPoller(Environment e,ObjectProvider<QueryGateway> gateway){
 this.gateway=gateway.getIfAvailable();events=ids(e.getProperty("apex.ops03.events",""));workflows=ids(e.getProperty("apex.ops03.workflows",""));
 String owners=e.getProperty("apex.ops03.owners","");
 if(!owners.isBlank())for(String owner:owners.split(",")){if(!owner.matches("[A-Za-z0-9._-]{1,64}")||caches.put(owner,new EvidenceCache())!=null||caches.size()>8)throw new IllegalArgumentException("INVENTORY");}
 if(!caches.isEmpty()&&this.gateway==null)throw new IllegalStateException("OWNER_TRANSPORT_REQUIRED");
 }
 @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
 public void start(){scheduler.scheduleWithFixedDelay(this::poll,0,5,TimeUnit.SECONDS);
 }
 private static List<UUID> ids(String s){if(s.isBlank())return List.of();var ids=Arrays.stream(s.split(",")).map(UUID::fromString).toList();if(ids.size()>16||new HashSet<>(ids).size()!=ids.size())throw new IllegalArgumentException("INVENTORY");return ids;}
 private void poll(){
 for(var entry:caches.entrySet()){
 String owner=entry.getKey();if(Instant.now().isBefore(next.getOrDefault(owner,Instant.EPOCH)))continue;
 try{
 var response=gateway.query(owner,new EvidenceQuery(events,workflows),OwnerSnapshot.class,new QueryOptions(UUID.randomUUID(),Duration.ofMillis(1000)));
 if(!response.owner().equals(owner)||response.facts().stream().anyMatch(f->f.kind()==OwnerSnapshot.Kind.HEALTH?false:f.kind()==OwnerSnapshot.Kind.WORKFLOW?!workflows.contains(f.ref()):!events.contains(f.ref())))throw new IllegalArgumentException("SOURCE_SCOPE");
 entry.getValue().accept(response,Instant.now());failures.remove(owner);next.remove(owner);
 }catch(Exception unavailable){int count=Math.min(4,failures.getOrDefault(owner,0)+1);failures.put(owner,count);next.put(owner,Instant.now().plusSeconds(5L*(1L<<count)));}
 }
 }
 public List<EvidenceCache.View> views(){return caches.entrySet().stream().map(e->e.getValue().view(e.getKey(),Instant.now())).toList();}
 public boolean eventAllowed(UUID id){return events.contains(id);}public boolean workflowAllowed(UUID id){return workflows.contains(id);}
 @Override public void close(){scheduler.shutdownNow();}
}

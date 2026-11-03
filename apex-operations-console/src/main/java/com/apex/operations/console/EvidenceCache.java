package com.apex.operations.console;
import com.apex.operations.contracts.*;import java.time.*;import java.util.*;
/** Finite current observations; previous divergent observations retained as bounded conflicts. */
public final class EvidenceCache {
 public record View(String source,Instant receivedAt,OwnerSnapshot snapshot,String freshness,String uncertainty,List<OwnerSnapshot> conflicts){}
 private OwnerSnapshot current;private Instant received;private final List<OwnerSnapshot> conflicts=new ArrayList<>();
 public synchronized void accept(OwnerSnapshot next,Instant now){
 if(current!=null&&(next.observedAt().isBefore(current.observedAt())||(next.observedAt().equals(current.observedAt())&&!next.equals(current)))){retain(next);return;}
 if(current!=null&&(!current.epoch().equals(next.epoch())||!current.facts().equals(next.facts())))retain(current);
 current=next;received=now;
 }
 private void retain(OwnerSnapshot s){if(conflicts.size()==2)conflicts.removeFirst();conflicts.add(s);}
 public synchronized View view(String source,Instant now){
 if(current==null)return new View(source,null,null,"MISSING","UNKNOWN",List.of());
 long age=Duration.between(current.observedAt(),now).toMillis();String freshness=age< -1000?"CLOCK_UNCERTAIN":age>current.maxAgeMillis()?"STALE":"FRESH";
 return new View(source,received,current,freshness,conflicts.stream().anyMatch(c->c.observedAt().equals(current.observedAt())&&!c.equals(current))?"CONFLICTING":"PARTIAL_MEMBERSHIP_NO_HISTORY",List.copyOf(conflicts));
 }
}

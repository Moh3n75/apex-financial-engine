package com.apex.operations.contracts;
import java.time.*;import java.util.*;
/** Current owner reads only. Not a historical journal or an atomic cross-table snapshot. */
public record OwnerSnapshot(int schemaVersion,String environment,String owner,String instance,UUID epoch,
 Instant observedAt,long maxAgeMillis,String coverage,List<Fact> facts,List<Attempt> attempts,
 boolean activeAvailable,long expiredAttempts,long rejectedAttempts) {
 public OwnerSnapshot {
 if(schemaVersion!=1||maxAgeMillis<1000||maxAgeMillis>60000||observedAt==null||epoch==null)throw new IllegalArgumentException("SCHEMA");
 for(String s:List.of(environment,owner,instance)) if(!s.matches("[A-Za-z0-9._-]{1,64}"))throw new IllegalArgumentException("SOURCE");
 if(!"EXPLICIT_KEYS_PARTIAL_NO_HISTORY".equals(coverage))throw new IllegalArgumentException("COVERAGE");
 facts=List.copyOf(facts);attempts=List.copyOf(attempts);if(facts.size()>128||attempts.size()>64)throw new IllegalArgumentException("SIZE");
 }
 public enum Kind { OUTBOX, INBOX, WORKFLOW, RECEIPT, HEALTH }
 public enum EvidenceClass { OWNER_COMMITTED_ROW, ATTEMPT_OBSERVATION }
 public enum Certainty { VERIFIED_WITHIN_SCOPE, OBSERVED_ONLY, UNKNOWN }
 public record Fact(Kind kind,UUID ref,String scope,String state,EvidenceClass evidenceClass,Certainty certainty,Instant sourceAssignedAt,String factKey) {
 public Fact(Kind kind,UUID ref,String scope,String state,EvidenceClass evidenceClass,Certainty certainty,Instant sourceAssignedAt){this(kind,ref,scope,state,evidenceClass,certainty,sourceAssignedAt,kind+":"+ref+":"+scope);}

 public Fact { if(!Objects.equals(factKey,kind+":"+ref+":"+scope))throw new IllegalArgumentException("FACT_KEY");Objects.requireNonNull(kind);Objects.requireNonNull(ref);Objects.requireNonNull(evidenceClass);Objects.requireNonNull(certainty);
 if(scope==null||!scope.matches("[A-Za-z0-9._-]{1,200}")||state==null||!state.matches("[A-Z_]{1,40}"))throw new IllegalArgumentException("FACT"); }

 }
 public record Attempt(UUID operationId,UUID epoch,String stage,long elapsedNanos) {
 public Attempt {if(operationId==null||epoch==null||stage==null||!stage.matches("[A-Z_]{1,40}")||elapsedNanos<0)throw new IllegalArgumentException("ATTEMPT");}
 }
}

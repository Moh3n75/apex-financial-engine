package com.apex.platform.discovery;
import java.time.Instant;
import java.util.*;
/** Infrastructure DTO: endpoints never cross the Console API boundary. */
public record CellMembership(String source,String registryIdentity,String coverage,Instant collectedAt,List<Member> members) {
  public record Member(String environment,String cellId,String instanceId,UUID epoch,String registryId,CellEndpoint endpoint) {}
  public CellMembership {members=List.copyOf(members);if(members.size()>64)throw new IllegalArgumentException("MEMBERSHIP_BOUND");}
  public boolean authoritative(){return coverage.equals("SCOPED_REGISTRY_COMPLETE");}
}

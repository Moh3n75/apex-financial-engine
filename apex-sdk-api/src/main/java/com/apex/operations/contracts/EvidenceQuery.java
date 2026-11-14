package com.apex.operations.contracts;
import com.apex.platform.grpc.api.CellQuery;
import java.util.*;
@CellQuery(type="OPS_OWNER_SNAPSHOT",version=1,response=OwnerSnapshot.class)
public record EvidenceQuery(List<UUID> events,List<UUID> workflows) {
 public EvidenceQuery { events=List.copyOf(events); workflows=List.copyOf(workflows);
 if(events.size()>16||workflows.size()>16||new HashSet<>(events).size()!=events.size()||new HashSet<>(workflows).size()!=workflows.size()) throw new IllegalArgumentException("QUERY_BOUND"); }
}

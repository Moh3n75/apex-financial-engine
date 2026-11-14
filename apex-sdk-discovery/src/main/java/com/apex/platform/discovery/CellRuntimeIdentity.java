package com.apex.platform.discovery;
import java.util.*;
/** One context incarnation shared by discovery registration and contract introspection. */
public record CellRuntimeIdentity(String environment,String cellId,String instanceId,UUID epoch) {
  public CellRuntimeIdentity {
    for(String value:List.of(environment,cellId,instanceId))if(!value.matches("[A-Za-z0-9._-]{1,64}"))throw new IllegalArgumentException("IDENTITY");
    Objects.requireNonNull(epoch);
  }
}

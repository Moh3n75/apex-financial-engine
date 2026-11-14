package com.apex.platform.grpc.api;

import java.util.List;

/** Register alongside a real caller, using the same destination and immutable Java DTO as the gateway invocation. */
public record GrpcCallerDeclarations(List<Dependency> dependencies) {
  public record Dependency(String destination, Class<?> contract,String behavior) {
    public Dependency(String destination,Class<?> contract){this(destination,contract,"REQUEST_RESPONSE");}
    public Dependency {
      if(destination==null || !destination.matches("[A-Za-z0-9._-]{1,64}") || contract==null
          || (contract.isAnnotationPresent(CellQuery.class)==contract.isAnnotationPresent(CellRequest.class)))
        throw new IllegalArgumentException("INVALID_CALLER_DECLARATION");
      if(!java.util.Set.of("REQUEST_RESPONSE","SYNCHRONOUS_REQUEST_RESPONSE","ASYNC_REQUEST_RESPONSE").contains(behavior))throw new IllegalArgumentException("INVALID_CALL_BEHAVIOR");
    }
  }
  public GrpcCallerDeclarations { dependencies=List.copyOf(dependencies);if(dependencies.size()>64)throw new IllegalArgumentException("BOUND"); }
}

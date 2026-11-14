package com.apex.platform.grpc.api;

/** allowPartial=false requires every target SUCCESS; outcomes remain available for diagnosis. */
public final class CollectionFailedException extends RuntimeException {
  private final CollectionResult<?> result;

  public CollectionFailedException(CollectionResult<?> result) {
    super("COLLECTION_INCOMPLETE");
    this.result = result;
  }

  public CollectionResult<?> result() {
    return result;
  }
}

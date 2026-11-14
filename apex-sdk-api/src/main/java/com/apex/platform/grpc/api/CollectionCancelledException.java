package com.apex.platform.grpc.api;

import java.util.concurrent.CancellationException;

/** Completed reads are retained; remaining slots are explicitly CANCELLED. */
public final class CollectionCancelledException extends CancellationException {
  private final CollectionResult<?> result;

  public CollectionCancelledException(CollectionResult<?> result) {
    super("COLLECTION_CANCELLED");
    this.result = result;
  }

  public CollectionResult<?> result() {
    return result;
  }
}

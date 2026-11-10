package com.apex.platform.contracts;

import com.apex.platform.events.api.EventContract;
import java.util.List;

/** Explicit catalog of the same immutable contracts used by business publishers. */
public record PublishedContracts(List<EventContract<?>> contracts) {
  public PublishedContracts { contracts = List.copyOf(contracts); }
}

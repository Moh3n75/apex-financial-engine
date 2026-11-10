package com.apex.platform.discovery;

import java.util.List;

/** Technical SPI only; business gateways retain their existing logical destination API. */
public interface CellServiceDirectory {
  List<CellEndpoint> endpoints(String logicalCell);
  default CellMembership membership(String logicalCell,String environment) {
    return new CellMembership("UNKNOWN","unknown","UNSUPPORTED_MEMBERSHIP",java.time.Instant.now(),List.of());
  }
}

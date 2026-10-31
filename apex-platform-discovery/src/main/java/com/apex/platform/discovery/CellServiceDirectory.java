package com.apex.platform.discovery;

import java.util.List;

/** Technical SPI only; business gateways retain their existing logical destination API. */
public interface CellServiceDirectory {
  List<CellEndpoint> endpoints(String logicalCell);
}

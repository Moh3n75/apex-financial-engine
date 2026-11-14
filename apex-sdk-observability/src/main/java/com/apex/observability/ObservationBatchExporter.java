package com.apex.observability;

import java.util.List;

/** Optional caller-provided export adapter. Runs only on isolated daemon export workers.
 * Implementations must honour interruption and their own finite I/O deadlines. No raw data. */
@FunctionalInterface
public interface ObservationBatchExporter {
    void export(List<OperationalObserver.Sample> samples) throws Exception;
}
